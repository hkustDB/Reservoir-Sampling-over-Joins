#include <query.h>
#include <utils.h>
#include <fstream>
#include <iostream>
#include <iomanip>

#include <tpc_ds_schema.h>

Query::Query(
    memmgr *mmgr,
    Join *join): 
    m_mmgr(mmgr),
    m_rec_mempool(mmgr),
    m_stables(mmgr),
    m_rec_max_len(mmgr),
    m_ts_colid(mmgr),
    m_join(join),
    m_exec_state(nullptr),
    m_window_type(WT_NOT_SET),
    m_window_size(0),
    m_num_tuples(0),
    m_filters(mmgr),
    m_is_no_dumping(false),
    m_max_key_constraint_warnings(10),
    m_report_event_handler(),
    m_meter_thread(),
    m_meter_stopped(true),
    m_start_time(),
    m_end_time() {
}

Query::~Query() {
    if (m_exec_state) {
        m_exec_state->~JoinExecState();
        m_mmgr->deallocate(m_exec_state);
    }

    if (m_join) {
        m_join->~Join();
        m_mmgr->deallocate(m_join);
    }

    for (SID sid = 0; sid < num_streams(); ++sid) {
        m_stables[sid]->~STable();
        m_mmgr->deallocate(m_stables[sid]);
        destroy_memmgr(m_rec_mempool[sid]);
    }
    m_rec_mempool.clear();
    m_stables.clear();
}

SID Query::add_stream(Schema *schema) {
    SID sid = num_streams();
    size_t max_len = schema->get_max_len();
    mempool_memmgr *mempool =
        create_memmgr<mempool_memmgr>(
            m_mmgr,
            max_len,
            mempool_memmgr::calc_block_size_from_block_bytes(
                1 * 1024 * 1024,
                max_len));
    
    m_rec_mempool.push_back(mempool);
    m_rec_max_len.push_back(max_len);
    m_stables.push_back(
        m_mmgr->newobj<STable>(
            m_mmgr,
            mempool,
            schema,
            sid));
    m_join->add_stream(schema);
    return sid;
}

void Query::add_equi_filter(SID sid, COLID colid, typeinfo *ti, Datum value) {
    m_filters.emplace_back(
    Filter::EQUI,
    sid,
    colid,
    ti,
    nullptr,
    value);    
}

void Query::add_neq_filter(SID sid, COLID colid, typeinfo *ti, Datum value) {
    m_filters.emplace_back(
    Filter::NEQ,
    sid,
    colid,
    ti,
    nullptr,
    value);
}

void Query::add_band_filter(SID sid, COLID colid, typeinfo *ti, Datum operand, Datum value) {
    m_filters.emplace_back(
    Filter::BAND,
    sid,
    colid,
    ti,
    operand,
    value);
}

void Query::set_tumbling_window(
    TIMESTAMP window_size) {
    
    m_window_type = WT_TUMBLING_WINDOW;
    m_window_size = window_size;
    m_join->set_window_non_overlapping();
    m_join->clear_is_unwindowed();
}

void Query::set_sliding_window(
    TIMESTAMP window_size) {
    
    m_window_type = WT_SLIDING_WINDOW;
    m_window_size = window_size;
    m_join->clear_window_non_overlapping();
    m_join->clear_is_unwindowed();
}

void Query::set_unwindowed(
    bool has_deletion) {
    
    m_window_type = WT_UNWINDOWED;
    m_join->set_is_unwindowed();
    if (!has_deletion) {
        // The join will maintain less state for purging
        // individual join samples.
        m_join->set_window_non_overlapping();
    } else {
        m_join->clear_window_non_overlapping();
    }
}

void Query::start_meter(std::string meter_filename) {
    if (!meter_filename.empty()) {
        m_meter_stopped = false;
        m_meter_thread = std::thread(&Query::run_meter, this, meter_filename);
    }
    m_start_time = std::chrono::system_clock::now();
}

void Query::run_meter(std::string meter_filename) {
    std::ofstream fout(meter_filename);
    fout << "time(s)\t\t\tntup\t\t\tnproc" << std::endl;

    time_point initial_time_point = std::chrono::system_clock::now();
    time_point next_time_point = initial_time_point;
    const std::chrono::duration interval = std::chrono::milliseconds(500);
    UINT8 last_num_tuples = 0;
    UINT8 last_num_processed = 0;

    while (!m_meter_stopped) {
        next_time_point += interval;
        std::this_thread::sleep_until(next_time_point);
        UINT8 num_tuples = read_num_tuples();
        UINT8 num_processed = m_exec_state->read_num_processed();
        
        UINT8 cnt = (UINT8) std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::system_clock::now() - initial_time_point).count();
        
        fout << cnt / 1000 << '.'
            << std::setfill('0') << std::setw(3) << cnt % 1000
            << std::setfill(' ') << std::setw(0)
            << "\t\t\t" << num_tuples - last_num_tuples
            << "\t\t\t" << num_processed - last_num_processed
            << std::endl;

        last_num_tuples = num_tuples;
        last_num_processed = num_processed;
    }
}

void Query::stop_meter() {
    m_end_time = std::chrono::system_clock::now();
    
    if (!m_meter_stopped) {
        m_meter_stopped = true;
        m_meter_thread.join();
    }

    UINT8 cnt = (UINT8) std::chrono::duration_cast<std::chrono::milliseconds>(
        m_end_time - m_start_time).count();
    std::cout << "execution duration = "
        << cnt / 1000 << '.'
        << std::setfill('0') << std::setw(3) << cnt % 1000
        << std::setfill(' ') << std::setw(0)
        << " s"
        << std::endl;
    
    UINT8 vmpeak = get_current_vmpeak();
    std::cout << "VmPeak = " << vmpeak << " KB = "
        << std::fixed << std::setprecision(3) << vmpeak / 1024.0 << " MB"
        << std::endl;
}

void Query::handle_report_event(
    std::ostream &result_out,
    TIMESTAMP min_ts,
    TIMESTAMP max_ts,
    UINT8 num_processed_since_last_report,
    UINT8 num_tuples_since_last_report) {
    
    if ((bool) m_report_event_handler) {
        m_report_event_handler(
            result_out,
            min_ts,
            max_ts,
            num_processed_since_last_report,
            num_tuples_since_last_report);
        return ;
    }

    auto iter_pair = this->get_samples();
    std::cout << "n_proc = " << num_processed_since_last_report
        << ", window [" << min_ts << ", " << max_ts << "]"
        << ", n_seen  = " << this->num_seen()
        << ", ssize = " << iter_pair.second - iter_pair.first
        << ", ntup = " << num_tuples_since_last_report
        << std::endl;
    result_out << "n_proc = " << num_processed_since_last_report
        << ", window [" << min_ts << ", " << max_ts << "]"
        << ", n_seen  = " << this->num_seen()
        << ", ssize = " << iter_pair.second - iter_pair.first
        << ", ntup = " << num_tuples_since_last_report
        << std::endl;

    // report execution time and memory usage
    stop_meter();
    
    Record rec;
    UINT8 i = 0;
    if (!this->m_is_no_dumping) {
        for (auto iter = iter_pair.first;
                iter != iter_pair.second;
                ++iter) {
            const JoinResult &jr = *iter;
            result_out << "sample " << i << ", ts = " << jr.m_ts << ":";
            for (SID sid = 0; sid < num_streams(); ++sid) {
                result_out << ' ' << jr.m_rowids[sid];
            }
            result_out << std::endl;

            ++i;
            // only print the first 10000
            if (i >= 10000) {
                result_out << "["
                    << iter_pair.second - iter_pair.first - 10000
                    << " samples omitted from output]"
                    << std::endl;
            }
        }
        result_out << std::endl;
    }
}

void SJoinQuery::execute(
    std::string input_filename,
    std::string result_filename,
    std::string meter_filename,
    UINT8 seed) {
    // TODO Should have checks with error returns instead of assertions.
    // But we are likely to refactor this function anyway, so don't
    // bother with that for now.
    RT_ASSERT(m_window_type != WT_NOT_SET);
    RT_ASSERT(m_window_type == WT_UNWINDOWED || m_window_size > 0);
    
    if (m_window_type != WT_UNWINDOWED) {
        // Tables are required to contain a ts column if there is
        // a tumbling or sliding window.
        m_ts_colid.reserve(m_stables.size());
        for (UINT8 i = 0; i < m_stables.size(); ++i) {
            const Schema *schema = m_stables[i]->get_schema();
            COLID ts_colid = schema->get_ts_colid();
            if (ts_colid == INVALID_COLID ||
                schema->get_type(ts_colid) != DTYPE_LONG) {
                RT_ASSERT(false); // TODO should return some error instead of assert
                return ;
            }
            m_ts_colid.push_back(ts_colid);
        }
    }

    m_exec_state = m_join->create_exec_state( 
                    m_mmgr,
                    m_stables.data(),
                    seed);
    m_sjoin_exec_state = dynamic_cast<SJoinExecState*>(m_exec_state);
    m_sjoin_exec_state->set_max_key_constraint_warnings(
        m_max_key_constraint_warnings);
    RT_ASSERT(m_exec_state->initialized());

    std_vector<Record> recs(m_mmgr, m_stables.size());
    for (SID sid = 0; sid < num_streams(); ++sid) {
        recs[sid].set_schema(m_stables[sid]->get_schema());
        recs[sid].set_buf(nullptr);
    }

    std::ofstream result_out(result_filename);
    
    // Don't use min_ts and max_ts if m_window_type == WT_UNWINDOWED.
    TIMESTAMP min_ts;
    TIMESTAMP max_ts;
    if (m_window_type == WT_TUMBLING_WINDOW) {
        min_ts = 0;
        max_ts = m_window_size - 1;
    } else if (m_window_type == WT_SLIDING_WINDOW) {
        max_ts = 0;
        min_ts = max_ts - m_window_size + 1;
    } else {
        min_ts = 0;
        max_ts = 0;
    }
    
    std::ifstream fin(input_filename);
    std::string line;

    UINT8 num_tuples_before_last_report = 0;
    UINT8 num_processed_before_last_report = 0;
    
    start_meter(meter_filename);
    while (std::getline(fin, line), !line.empty()) {
        // Someone Ctrl-C'd (SIGTERM singaled) us. Break early.
        if (g_terminated) break;
    
        // A single "-" on a single line is a sample request.
        const char *cline = line.c_str();
        if (*cline == '-') {
        UINT8 num_processed_so_far = m_exec_state->num_processed();
        handle_report_event(
                result_out,
                min_ts,
                max_ts,
                num_processed_so_far - num_processed_before_last_report,
                m_num_tuples - num_tuples_before_last_report);
            num_processed_before_last_report = num_processed_so_far;
            num_tuples_before_last_report = m_num_tuples;
            continue;
        }
    
        // TPC-DS tuple deletion: currently only supports pop_front
        // "d <SID> <num_tups_to_delete>"
        // TODO implement tuple look ups in STable and vertex nodes
        // so that we can delete arbitrary tuples
        if (*cline == 'd') {
            SID sid = 0;
            UINT8 num_tups_to_delete = 0;
            while (*++cline == ' ');
            while (*cline != ' ') {
                sid = sid * 10 + *cline - '0';
                ++cline;
            }
            while (*++cline == ' ');
            while (*cline != '\0') {
                num_tups_to_delete = num_tups_to_delete * 10 + *cline - '0'; 
                ++cline;
            }
            if (num_tups_to_delete > m_stables[sid]->num_records()) {
                num_tups_to_delete = m_stables[sid]->num_records();
                if (!num_tups_to_delete) continue; // nothing left in the table already
            }

            //std::cout << sid << " " << num_tups_to_delete << std::endl;            
            m_exec_state->expire_multiple_and_retire_samples(
                    sid, m_stables[sid]->base_rowid(), num_tups_to_delete);
            m_stables[sid]->expire_front_records_only(num_tups_to_delete);

            // We count deletion towards the number of inserted tuples as well.
            // It's actually number of SQL ops.
            m_num_tuples += num_tups_to_delete;
            continue;
        }

        if (measure_update_only) {
            tuple_start_time = std::chrono::system_clock::now();
        }
        ++m_num_tuples;
        SID sid = 0;
        while (*cline != '|') {
            sid = sid * 10 + *cline - '0';
            ++cline;
        }
        ++cline;
    
        Record *rec = &recs[sid];
        BYTE *buf = m_rec_mempool[sid]->allocate_array<BYTE>(m_rec_max_len[sid]);
        rec->set_buf(buf);
        rec->parse_line(cline, ',');

        bool is_filtered_out = false;
        for (auto filter : m_filters) {
            if (filter.get_sid() == sid && !filter.match(rec)) {
            is_filtered_out = true;
            break;
            }    
        }
        if (is_filtered_out) {
            -- m_num_tuples;
                continue;
        }
        
        if (m_window_type != WT_UNWINDOWED) {
            TIMESTAMP ts = rec->get_long(m_ts_colid[sid]);
            if (ts > max_ts) {
                if (m_window_type == WT_TUMBLING_WINDOW) {
                    min_ts = ts / m_window_size * m_window_size;
                    max_ts = min_ts + m_window_size - 1;
                    for (SID sid = 0; sid < num_streams(); ++sid) {
                        m_stables[sid]->expire_all();
                    }
                    m_exec_state->expire_all();
                } else {
                    max_ts = ts;
                    min_ts = ts - m_window_size + 1;
                    for (SID sid = 0; sid < num_streams(); ++sid) {
                        m_stables[sid]->expire(min_ts);
                    }
                    m_exec_state->set_min_ts(min_ts); 
                }
            }
        }

        (void) m_stables[sid]->notify(rec);
        if(measure_update_only) {
            tuple_end_time = std::chrono::system_clock::now();
            auto update_time = std::chrono::duration_cast<std::chrono::nanoseconds>(tuple_end_time - tuple_start_time).count();
            total_update_time += update_time;
            update_times.emplace_back(update_time);
        }
    }
    stop_meter();
    if (measure_update_only) {
        printf("Total Update Time: %lu ns\n", total_update_time);
        for (auto t : update_times) {
            std::cout << t << std::endl;
        }
    }

    m_exec_state->~JoinExecState();
    m_mmgr->deallocate(m_exec_state);
    m_exec_state = nullptr;
}

void SJoinQuery::execute_minibatch(
    std::string input_filename,
    std::string result_filename,
    std::string meter_filename,
    UINT8 batch_size,
    UINT8 seed) {
    
    // TODO Should have checks with error returns instead of assertions.
    // But we are likely to refactor this function anyway, so don't
    // bother with that for now.
    RT_ASSERT(m_window_type != WT_NOT_SET);
    RT_ASSERT(m_window_type == WT_UNWINDOWED || m_window_size > 0);

    if (m_window_type != WT_UNWINDOWED) {
        // Tables are required to contain a ts column if there is
        // a tumbling or sliding window.
        m_ts_colid.reserve(m_stables.size());
        for (UINT8 i = 0; i < m_stables.size(); ++i) {
            const Schema *schema = m_stables[i]->get_schema();
            COLID ts_colid = schema->get_ts_colid();
            if (ts_colid == INVALID_COLID ||
                schema->get_type(ts_colid) != DTYPE_LONG) {
                RT_ASSERT(false); // TODO should return some error instead of assert
                return ;
            }
            m_ts_colid.push_back(ts_colid);
        }
    }
    
    m_exec_state = m_join->create_exec_state( 
                    m_mmgr,
                    m_stables.data(),
                    seed);
    m_sjoin_exec_state = dynamic_cast<SJoinExecState*>(m_exec_state);
    m_sjoin_exec_state->set_max_key_constraint_warnings(
        m_max_key_constraint_warnings);
    RT_ASSERT(m_exec_state->initialized());
    
    /* 
     * Pooled rows are put in num_streams() of circular queues.
     * Tuple (sid, i) is at
     * sid * batch_size + (pooled_recs_first + i) % batch_size.
     */
    std_vector<Record> recs(m_mmgr, num_streams() * batch_size);
    for (SID sid = 0; sid < num_streams(); ++sid) {
        for (UINT8 i = 0; i < batch_size; ++i) {
            UINT8 idx = sid * batch_size + i;
            recs[idx].set_schema(m_stables[sid]->get_schema());
            recs[sid].set_buf(nullptr);
        }
    }
    std_vector<UINT8> num_pooled_recs(m_mmgr, num_streams(), 0);
    std_vector<UINT8> pooled_recs_first(m_mmgr, num_streams(), 0);

    std::ofstream result_out(result_filename);

    // Don't use min_ts and max_ts if m_window_type == WT_UNWINDOWED.
    TIMESTAMP min_ts;
    TIMESTAMP max_ts;
    if (m_window_type == WT_TUMBLING_WINDOW) {
        min_ts = 0;
        max_ts = m_window_size - 1;
    } else if (m_window_type == WT_SLIDING_WINDOW) {
        max_ts = 0;
        min_ts = max_ts - m_window_size + 1;
    } else {
        min_ts = 0;
        max_ts = 0;
    }

    std::ifstream fin(input_filename);
    std::string line;

    UINT8 num_tuples_before_last_report = 0;
    UINT8 num_processed_before_last_report = 0;
    
    start_meter(meter_filename);

    while (std::getline(fin, line), !line.empty()) {
        if (g_terminated) break;

        const char *cline = line.c_str();
        if (*cline == '-') {
            /* update and clear the pooled records before reporting */
            for (SID sid = 0; sid < num_streams(); ++sid) {
                if (num_pooled_recs[sid] != 0) {
                    m_stables[sid]->notify_minibatch(
                        &recs[sid * batch_size],
                        pooled_recs_first[sid],
                        num_pooled_recs[sid]);
                    pooled_recs_first[sid] = 0;
                    num_pooled_recs[sid] = 0;
                }
            }

            /* report */
            UINT8 num_processed_so_far = m_exec_state->num_processed();
            handle_report_event(
                result_out,
                min_ts,
                max_ts,
                num_processed_so_far - num_processed_before_last_report,
                m_num_tuples - num_tuples_before_last_report);
            num_processed_before_last_report = num_processed_so_far;
            num_tuples_before_last_report = m_num_tuples;
            continue;
        }

        // TODO handle 'd' command

        ++m_num_tuples;

        SID sid = 0;
        while (*cline != '|') {
            sid = sid * 10 + *cline - '0';
            ++cline;
        }
        ++cline;

        UINT8 rec_buf_idx = sid * batch_size
            + (pooled_recs_first[sid] + num_pooled_recs[sid]) % batch_size;
        Record *rec = &recs[rec_buf_idx];
        BYTE *buf = m_rec_mempool[sid]->allocate_array<BYTE>(m_rec_max_len[sid]);
        rec->set_buf(buf);
        rec->parse_line(cline, ',');

        bool is_filtered_out = false;
        for (auto filter : m_filters) {
            if (filter.get_sid() == sid && !filter.match(rec)) {
                is_filtered_out = true;
                break;
            }
        }
        if (is_filtered_out) {
            -- m_num_tuples;
            continue;
        }

        
        if (m_window_type != WT_UNWINDOWED) {
            TIMESTAMP ts = rec->get_long(m_ts_colid[sid]);
            if (ts > max_ts) {
                if (m_window_type == WT_TUMBLING_WINDOW) {
                    min_ts = ts / m_window_size * m_window_size;
                    max_ts = min_ts + m_window_size - 1;
                    for (SID sid = 0; sid < num_streams(); ++sid) {
                        m_stables[sid]->expire_all();
                    }
                    m_exec_state->expire_all();

                    /* 
                     * The current tuple is the only one left after we
                     * move the window. Discard everything else.
                     */
                    for (SID sid2 = 0; sid2 < num_streams(); ++sid2) {
                        /* num_pooled_recs[sid] has not been incremented at this
                         * point so it's okay to deallocate records up to
                         * num_pooled_recs[sid] */
                        for (UINT8 i = 0; i < num_pooled_recs[sid2]; ++i) {
                            UINT8 expired_tup_idx = sid2 * batch_size
                                + (i + pooled_recs_first[sid2]) % batch_size;
                            m_rec_mempool[sid2]->deallocate(recs[expired_tup_idx].get_buf());
                            recs[expired_tup_idx].set_buf(nullptr);
                        }

                        if (sid2 == sid) {
                            pooled_recs_first[sid] = (pooled_recs_first[sid] + num_pooled_recs[sid]) % batch_size;
                        } else {
                            pooled_recs_first[sid2] = 0;
                        }
                        num_pooled_recs[sid2] = 0;
                    }
                } else {
                    max_ts = ts;
                    min_ts = ts - m_window_size + 1;
                    for (SID sid = 0; sid < num_streams(); ++sid) {
                        max_ts = ts;
                        min_ts = ts - m_window_size + 1;
                        for (SID sid = 0; sid < num_streams(); ++sid) {
                            m_stables[sid]->expire_minibatch(min_ts);
                        }
                        m_exec_state->set_min_ts(min_ts);
                    }

                    /* Remove from the rec pools expired rows. */
                    for (SID sid2 = 0; sid2 < num_streams(); ++sid2) {
                        for (UINT8 i = 0; i < num_pooled_recs[sid2]; ++i) {
                            UINT8 tup_idx = sid2 * batch_size
                                + (i + pooled_recs_first[sid2]) % batch_size;
                            if (recs[tup_idx].get_long(m_ts_colid[sid]) < min_ts) {
                                m_rec_mempool[sid2]->deallocate(recs[tup_idx].get_buf());
                                recs[tup_idx].set_buf(nullptr);
                                ++pooled_recs_first[sid2];
                            } else {
                                break;
                            }
                        }
                    }
                }
            }
        }

        ++num_pooled_recs[sid];
        if (num_pooled_recs[sid] == batch_size) {
            /* update the pooled recs in stream sid */
            m_stables[sid]->notify_minibatch(
                &recs[sid * batch_size],
                pooled_recs_first[sid],
                batch_size);
            pooled_recs_first[sid] = 0;
            num_pooled_recs[sid] = 0;
        }
    }

    stop_meter();

    m_exec_state->~JoinExecState();
    m_mmgr->deallocate(m_exec_state);
    m_exec_state = nullptr;
}

void SINLJoinQuery::execute(
    std::string input_filename,
    std::string result_filename,
    std::string meter_filename,
    UINT8 seed) {

    // TODO Should have checks with error returns instead of assertions.
    // But we are likely to refactor this function anyway, so don't
    // bother with that for now.
    RT_ASSERT(m_window_type != WT_NOT_SET);
    RT_ASSERT(m_window_type == WT_UNWINDOWED || m_window_size > 0);

    if (m_window_type != WT_UNWINDOWED) {
        // Tables are required to contain a ts column if there is
        // a tumbling or sliding window.
        m_ts_colid.reserve(m_stables.size());
        for (UINT8 i = 0; i < m_stables.size(); ++i) {
            const Schema *schema = m_stables[i]->get_schema();
            COLID ts_colid = schema->get_ts_colid();
            if (ts_colid == INVALID_COLID ||
                schema->get_type(ts_colid) != DTYPE_LONG) {
                RT_ASSERT(false); // TODO should return some error instead of assert
                return ;
            }
            m_ts_colid.push_back(ts_colid);
        }
    }
    
    m_exec_state = m_join->create_exec_state( 
                    m_mmgr,
                    m_stables.data(),
                    seed);
    m_sinljoin_exec_state = dynamic_cast<SINLJoinExecState*>(m_exec_state);
    // FIXME this currently has no effect
    m_sinljoin_exec_state->set_max_key_constraint_warnings(
        m_max_key_constraint_warnings);

    RT_ASSERT(m_exec_state->initialized());
    
    std_vector<Record> recs(m_mmgr, m_stables.size());
    for (SID sid = 0; sid < num_streams(); ++sid) {
        recs[sid].set_schema(m_stables[sid]->get_schema());
        recs[sid].set_buf(nullptr);
    }

    std::ofstream result_out(result_filename);

    // Don't set/use min_ts and max_ts if m_window_type == WT_UNWINDOWED.
    TIMESTAMP min_ts;
    TIMESTAMP max_ts;
    if (m_window_type == WT_TUMBLING_WINDOW) {
        min_ts = 0;
        max_ts = m_window_size - 1;
    } else if (m_window_type == WT_SLIDING_WINDOW) {
        max_ts = 0;
        min_ts = max_ts - m_window_size + 1;
    } else {
        min_ts = 0;
        max_ts = 0;
    }
    
    std::ifstream fin(input_filename);
    std::string line;

    UINT8 num_tuples_before_last_report = 0;
    UINT8 num_processed_before_last_report = 0;
    
    start_meter(meter_filename);

    while (std::getline(fin, line), !line.empty()) {
        if (g_terminated) break;
        const char *cline = line.c_str();
        if (*cline == '-') {
        UINT8 num_processed_so_far = m_exec_state->num_processed();
            handle_report_event(
                result_out,
                min_ts,
                max_ts,
                num_processed_so_far - num_processed_before_last_report,
                m_num_tuples - num_tuples_before_last_report);
            num_processed_before_last_report = num_processed_so_far;
            num_tuples_before_last_report = m_num_tuples;
            continue;
        }

        if (*cline == 'd') {
            /* TPC-DS tuple deletion: currently only supports pop_front */
            /* "d <SID> <num_tups_to_delete>"*/
            SID sid = 0;
            UINT8 num_tups_to_delete = 0;
            while (*++cline == ' ');
            while (*cline != ' ') {
                sid = sid * 10 + *cline - '0';
                ++cline;
            }
            while (*++cline == ' ');
            while (*cline != '\0') {
                num_tups_to_delete = num_tups_to_delete * 10 + *cline - '0'; 
                ++cline;
            }
            
            // both of the following two methods are bad choices for
            // overloading in SINLJoinExecState: The only effect observable to
            // the outside is resets the iterator for reiteration
            m_exec_state->expire_multiple_and_retire_samples(
                    sid, m_stables[sid]->base_rowid(), num_tups_to_delete);
            if (m_window_type != WT_UNWINDOWED) {
                m_exec_state->set_min_ts(0); // doesn't matter what value is passed
            }
            handle_deletion(sid, m_stables[sid]->base_rowid(), num_tups_to_delete);

            m_stables[sid]->expire_front_records_only(num_tups_to_delete);

            // We count deletion towards the number of inserted tuples as well.
            // It's actually number of SQL ops.
            m_num_tuples += num_tups_to_delete;
            continue;
        }

        ++m_num_tuples;
        SID sid = 0;
        while (*cline != '|') {
            sid = sid * 10 + *cline - '0';
            ++cline;
        }
        ++cline;

        Record *rec = &recs[sid];
        BYTE *buf = m_rec_mempool[sid]->allocate_array<BYTE>(m_rec_max_len[sid]);
        rec->set_buf(buf);
        rec->parse_line(cline, ',');

        bool is_filtered_out = false;
        for (auto filter : m_filters) {
            if (filter.get_sid() == sid && !filter.match(rec)) {
                is_filtered_out = true;
                break;
            }
        }
        if (is_filtered_out) {
            -- m_num_tuples;
            continue;
        }
        
        if (m_window_type != WT_UNWINDOWED) {
            TIMESTAMP ts = rec->get_long(m_ts_colid[sid]);
            if (ts > max_ts) {
                if (m_window_type == WT_TUMBLING_WINDOW) {
                    min_ts = ts / m_window_size * m_window_size;
                    max_ts = min_ts + m_window_size - 1;
                    for (SID sid = 0; sid < num_streams(); ++sid) {
                        m_stables[sid]->expire_all();
                    }
                    m_exec_state->expire_all();
                    handle_tumbling_window_expiration();
                } else {
                    max_ts = ts;
                    min_ts = ts - m_window_size + 1;
                    for (SID sid = 0; sid < num_streams(); ++sid) {
                        m_stables[sid]->expire_downstream_only(min_ts);
                    }

                    m_exec_state->init_join_result_iterator();
                    while (m_exec_state->has_next_join_result()) {
                        const JoinResult &jr = m_exec_state->next_join_result();
                        process_next_join_result_during_expiration(jr);
                    }
                
                    for (SID sid = 0; sid < num_streams(); ++sid) {
                        m_stables[sid]->expire_records_only(min_ts);
                    }
                    
                    // we use set_min_ts to reiterate through
                    // all active join results if needed by subclass
                    m_exec_state->set_min_ts(min_ts);
                    handle_sliding_window_expiration(min_ts);
                }
            }
        }

        m_stables[sid]->notify(rec);
        m_exec_state->init_join_result_iterator();
        while (m_exec_state->has_next_join_result()) {
                const JoinResult &jr = m_exec_state->next_join_result();
                process_next_join_result(jr);
        }
    }

    stop_meter();

    m_exec_state->~JoinExecState();
    m_mmgr->deallocate(m_exec_state);
    m_exec_state = nullptr;
}

SINLJoinSamplingQuery::SINLJoinSamplingQuery(
    memmgr *mmgr)
    : SINLJoinQuery(mmgr),
    m_samples(mmgr),
    m_sampling_type(0),
    m_sampling_param({.m_sampling_rate = 0}),
    m_generic_mempool(create_memmgr<generic_mempool_memmgr>(mmgr, 1 * 1024 * 1024)),
    m_rng(),
    m_unif_0_1(0,1),
    m_unif_sample_size(0,0) {}

SINLJoinSamplingQuery::~SINLJoinSamplingQuery() {
    destroy_memmgr(m_generic_mempool);
}

void SINLJoinSamplingQuery::execute(
    std::string input_filename,
    std::string result_filename,
    std::string meter_filename,
    UINT8 seed) {
    
    m_rng.seed(seed);
    
    switch (m_sampling_type) {
    case 'B':
        m_samples.reserve(262144);
        m_samples.clear();
        m_sinljoin->clear_require_iterating_expired_join_results();
        m_sinljoin->clear_require_reiterating_active_join_results();
        break;
    case 'W':
        m_samples.reserve(m_sampling_param.m_sample_size);
        m_samples.clear();
        m_sinljoin->clear_require_iterating_expired_join_results();
        if (m_window_type != WT_UNWINDOWED &&
            !m_join->is_window_non_overlapping()) {
            m_sinljoin->set_require_reiterating_active_join_results();
        } else {
            m_sinljoin->clear_require_reiterating_active_join_results();
        }
        break;
    case 'O':
        m_samples.reserve(m_sampling_param.m_sample_size);
        m_samples.clear();
        m_unif_sample_size = std::uniform_int_distribution<UINT8>(
            0, m_sampling_param.m_sample_size - 1);

        m_sinljoin->clear_require_iterating_expired_join_results();
        if (m_window_type != WT_UNWINDOWED &&
            !m_join->is_window_non_overlapping()) {
            m_sinljoin->set_require_reiterating_active_join_results();
        } else {
            m_sinljoin->clear_require_reiterating_active_join_results();
        }

        break;
    default:
        RT_ASSERT(false);
    }

    SINLJoinQuery::execute(input_filename, result_filename, meter_filename);
}


void SINLJoinSamplingQuery::process_next_join_result(const JoinResult &jr) {
    switch (m_sampling_type) {
    case 'B':
        {
            if (m_unif_0_1(m_rng) < m_sampling_param.m_sampling_rate) {
                m_samples.push_back(copy_join_result(jr));
                if (!m_join->is_window_non_overlapping() && !m_join->is_unwindowed()) {
                    std::push_heap(
                        m_samples.begin(),
                        m_samples.end(),
                        std::greater<JoinResult>());
                }
            }
        }
        break;
    case 'W':
        {
            UINT8 t = m_sinljoin_exec_state->num_seen();
            if (t == 1) {
                for (UINT8 i = 0; i < m_sampling_param.m_sample_size; ++i) {
                    m_samples.push_back(copy_join_result(jr));
                }
            } else {
                DOUBLE threshold = 1.0 / t;
                for (UINT8 i = 0; i < m_sampling_param.m_sample_size; ++i) {
                    if (m_unif_0_1(m_rng) < threshold) {
                        m_samples[i].m_ts = jr.m_ts;
                        memcpy(m_samples[i].m_rowids, jr.m_rowids, sizeof(ROWID) * num_streams()); 
                    }
                }
            }
        }
        break;
    case 'O':
        {
            // this is actually t + 1
            UINT8 t = m_sinljoin_exec_state->num_seen();
            if (t <= m_sampling_param.m_sample_size) {
                m_samples.push_back(copy_join_result(jr));
            } else if (m_unif_0_1(m_rng)
                < ((DOUBLE) m_sampling_param.m_sample_size) / t) {
                
                UINT8 idx = m_unif_sample_size(m_rng);
                ROWID *old_rowids = m_samples[idx].m_rowids;
                m_generic_mempool->deallocate(old_rowids);
                m_samples[idx] = copy_join_result(jr);
            }
        }
        break;
    default:
        RT_ASSERT(false);
    }
}

void SINLJoinSamplingQuery::process_next_join_result_during_expiration(
    const JoinResult &jr) {
    
    // should be unreachable
    RT_ASSERT(false);
}

void SINLJoinSamplingQuery::handle_tumbling_window_expiration() {
    switch (m_sampling_type) {
    case 'B':
    case 'W':
    case 'O':
        m_generic_mempool->deallocate_all();
        m_samples.clear();
        break;
    default:
        RT_ASSERT(false);
    }
}

void SINLJoinSamplingQuery::handle_sliding_window_expiration(
    TIMESTAMP min_ts) {
    
    switch (m_sampling_type) {
    case 'B':
        {
            while (!m_samples.empty() && m_samples.front().m_ts < min_ts) {
                m_generic_mempool->deallocate(m_samples.front().m_rowids);
                std::pop_heap(
                    m_samples.begin(),
                    m_samples.end(),
                    std::greater<JoinResult>());
                m_samples.pop_back();
            }
        }
        break;
    case 'W':
    case 'O':
        {
            // have to reiterate through all active join results
            m_generic_mempool->deallocate_all();
            m_samples.clear();

            m_exec_state->init_join_result_iterator();
            while (m_exec_state->has_next_join_result()) {
                const JoinResult &jr = m_exec_state->next_join_result();
                process_next_join_result(jr);
            }
        }
        break;
    default:
        RT_ASSERT(false);
    }
}

void SINLJoinSamplingQuery::handle_deletion(
    SID sid, ROWID start_rowid, UINT8 num_rows) {
    
    if (m_sampling_type != 'B') {
        handle_sliding_window_expiration(0); // doesn't matter what value is passed
        return ;
    }

    // partition and purge the expired samples
    UINT8 min_rowid = start_rowid + num_rows;
    auto new_end = std::partition(
            m_samples.begin(),
            m_samples.end(),
            [&](const JoinResult &jr) -> bool {
                return jr.m_rowids[sid] >= min_rowid;
            });
    for (auto iter = new_end; iter != m_samples.end(); ++iter) {
        m_generic_mempool->deallocate(iter->m_rowids);
    }
    m_samples.erase(new_end, m_samples.end());
}

