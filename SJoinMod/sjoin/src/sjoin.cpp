#include <sjoin.h>
#include <debug.h>
#include <min_heap.h>
#include <algorithm>
#include <iostream>

SJoin::SJoin(memmgr *mmgr)
    : Join(mmgr),
    m_sampling_type(UNSPECIFIED_SAMPLING_TYPE)
    // , m_sampling_param() // deliberately skipped
{}


SJoin::~SJoin() {
}


JoinExecState *SJoin::create_exec_state(memmgr *mmgr, STable **stables, UINT8 seed) {
    SJoinExecState *sjoin_exec_state = nullptr;
    sjoin_exec_state = mmgr->newobj<SJoinExecState>(
                        mmgr,
                        this,
                        stables,
                        seed);
    if (!sjoin_exec_state->initialized()) {
        sjoin_exec_state->~SJoinExecState();
        mmgr->deallocate(sjoin_exec_state);
        return nullptr;
    }
    for (SID sid = 0; sid < num_streams(); ++sid) {
        stables[sid]->set_downstream_exec_state(sjoin_exec_state);
    }
    return sjoin_exec_state;
}

SJoinExecState::SJoinExecState(
    memmgr *mmgr,
    SJoin *sjoin,
    STable **stables,
    UINT8 seed)
    : m_mmgr(mmgr),
      m_n_streams(sjoin->num_streams()),
      m_window_non_overlapping(sjoin->m_window_non_overlapping),
      m_is_unwindowed(sjoin->m_is_unwindowed),
      m_sampling_type(sjoin->m_sampling_type),
      m_sampling_param(sjoin->m_sampling_param),
      m_stables(stables),
      m_generic_mempool(nullptr),
      m_predicates(m_mmgr),
      m_gq_vertices(nullptr),
      m_vertex_mempool(nullptr),
      m_vertices(nullptr),
      m_vertex_hash_table(nullptr),
      m_consolidated_table_tuple_hash_table(nullptr),
      m_n_indexes(0),
      m_index_info(nullptr),
      m_join_sample{.m_rowids = nullptr},
      m_reservoir(m_mmgr),
      m_num_seen(0),
      m_skip_number(0),
      m_rng(seed),
      m_walker_param(nullptr),
      m_reservoir_iter(),
      m_sample_set(
        m_mmgr,
        16, // dummy value, will be reset in ctor
        ROWIDArrayHash(m_n_streams),
        ROWIDArrayEqual(m_n_streams)),
      m_rowid2sample_idx(nullptr),
      m_algo_Z_unif(0.0, 1.0),
      m_unif_sample_size(0,0),
      m_algo_Z_threshold(0),
      m_min_heap_nproc(m_mmgr),
      m_min_heap_ts(m_mmgr),
      m_sample_idx2min_heap_ts_idx(m_mmgr),
      m_foreign_key_refs(m_mmgr),
      m_rowids_for_projection(nullptr),
      m_num_key_constraint_warnings(0),
      m_max_key_constraint_warnings(m_default_max_key_constraint_warnings) {

    // generic mempool: 1 MB blocks
    m_generic_mempool = create_memmgr<generic_mempool_memmgr>(m_mmgr, 1 * 1024 * 1024);

    m_rowids_for_projection = m_mmgr->allocate_array<ROWID>(m_n_streams);

    // make a copy of the predicate pointers
    m_predicates.reserve(sjoin->m_predicates.size());
    for (Predicate &pred : sjoin->m_predicates) {
        m_predicates.push_back(&pred);
    }

    // initialize vertices
    m_gq_vertices = m_mmgr->allocate_array<SJoinQueryGraphVertex>(m_n_streams);
    memset(m_gq_vertices, 0, sizeof(SJoinQueryGraphVertex) * m_n_streams);
    for (SID sid = 0; sid < m_n_streams; ++sid){
        m_gq_vertices[sid].m_sid = sid;
    }

    if (!consolidate_key_joins()) goto fail;

    if (!create_query_graph()) goto fail;

    if (!create_node_layout()) goto fail;

    if (!create_indexes()) goto fail;

    m_vertices = m_mmgr->allocate_array<SJoinJoinGraphVertex*>(m_n_streams);
    memset(m_vertices, 0, sizeof(SJoinJoinGraphVertex*) * m_n_streams);

    if (m_sampling_param.m_sample_size > 0) {
        if (!initialize_sampling()) goto fail;
    }

    return ;
fail:
    clean_all();
}

// Try to consolidate tables connected by key join predicates.
// Remove all such predicates from m_predicates upon return.
bool SJoinExecState::consolidate_key_joins() {
    m_foreign_key_refs.reserve(m_n_streams);
    for (SID sid = 0; sid < m_n_streams; ++sid) {
        m_foreign_key_refs.emplace_back(
            ForeignKeyRef{INVALID_SID, INVALID_COLID});
    }

    std_vector<Predicate*> applicable_predicates(m_mmgr);
    applicable_predicates.reserve(m_predicates.size());
    UINT8 n_remaining_predicates = 0;
    for (UINT8 i = 0; i < m_predicates.size(); ++i) {
        Predicate *pred = m_predicates[i];
        if (pred->get_fkey_spec() != Predicate::NO_FKEY) {
            // This is an applicable key-join predicate, move it
            // from m_preciates into applicable_predicates.
            applicable_predicates.push_back(pred);
        } else {
            // No, keep it.
            m_predicates[n_remaining_predicates++] = pred;
        }
    }
    m_predicates.resize(n_remaining_predicates);

    // The loop examines all the pkey-fkey predicates, and determine the
    // directions of lookups. If some pkey-fkey predicate is not applicable
    // because it conflicts with some other in the pkey side (e.g., A --x-- B
    // --x-- C, where x is a primary key only for B, then we can only choose to
    // look up B from either A or C but not both), it is put back into
    // m_predicates.
    for (Predicate *pred: applicable_predicates) {
        SID lsid = pred->get_lsid();
        SID rsid = pred->get_rsid();
        const Schema *lsch = m_stables[lsid]->get_schema();
        const Schema *rsch = m_stables[rsid]->get_schema();
        COLID lcolid = pred->get_lcolid();
        COLID rcolid = pred->get_rcolid();

        if (pred->get_fkey_spec() == Predicate::RIGHT_IS_FKEY) {
            // pkey-fkey predicate
            using std::swap;
            swap(lsid, rsid);
            swap(lsch, rsch);
            swap(lcolid, rcolid);
        } else {
            RT_ASSERT(pred->get_fkey_spec() == Predicate::LEFT_IS_FKEY);
        }

        // fkey-pkey predicate or a swapped pkey-fkey predicate
        RT_ASSERT(rcolid == rsch->get_primary_key_colid());
        if (m_foreign_key_refs[rsid].m_sid != INVALID_SID) {
            // a conflict, put pred back into m_predicates
            m_predicates.push_back(pred);
            continue;
        }
        m_foreign_key_refs[rsid].m_sid = lsid;
        m_foreign_key_refs[rsid].m_colid = lcolid;
    }

    // m_gq_vertices[.].m_sid: child linked-list next link
    // m_gq_vertices[.].m_root_sid: first child link
    // m_foreign_key_refs[.].m_sid: parent link

    // Build tree.
    SID root_level_head = INVALID_SID;
    for (SID sid = 0; sid < m_n_streams; ++sid) {
        m_gq_vertices[sid].m_sid = INVALID_SID;
        m_gq_vertices[sid].m_root_sid = INVALID_SID;
    }
    for (SID sid = 0; sid < m_n_streams; ++sid) {
        if (m_foreign_key_refs[sid].m_sid == INVALID_SID) {
            // This is an unconsolidated (root) table.
            m_gq_vertices[sid].m_sid = root_level_head;
            root_level_head = sid;
        } else {
            // This is a consolidated table.
            SID parent_sid = m_foreign_key_refs[sid].m_sid;
            m_gq_vertices[sid].m_sid = m_gq_vertices[parent_sid].m_root_sid;
            m_gq_vertices[parent_sid].m_root_sid = sid;
        }
    }

    // Run two passes over the tree.
    //
    // The first pass only computes m_n_child_tables.
    //
    // The second pass allocates all the m_child_table_sid arrays, fills all
    // the consolidated tables into the m_child_table_sid arrays in pre-order
    // and fix m_sid and m_root_sid.
    constexpr const int dry_run_pass = 0;
    for (int pass = 0; pass < 2; ++pass) {
        SID root_sid = root_level_head;
        while (root_sid != INVALID_SID) {
            if (pass == dry_run_pass) {
                m_gq_vertices[root_sid].m_n_child_tables = 0;
            } else {
                if (m_gq_vertices[root_sid].m_n_child_tables != 0) {
                    m_gq_vertices[root_sid].m_child_table_sid =
                        m_mmgr->allocate_array<SID>(
                            m_gq_vertices[root_sid].m_n_child_tables);

                    // m_n_child_tables serves as the iterator index
                    // for insertion into m_child_table_sid
                    DARG0(m_gq_vertices[root_sid].m_n_child_tables_copy =
                        m_gq_vertices[root_sid].m_n_child_tables);
                    m_gq_vertices[root_sid].m_n_child_tables = 0;
                }
            }

            // Pre-order traversal on the tree
            SID cur_sid = m_gq_vertices[root_sid].m_root_sid;
            if (cur_sid != INVALID_SID) {
                while (cur_sid != root_sid) {
                    SID next_sid = m_gq_vertices[cur_sid].m_root_sid;
                    if (next_sid == INVALID_SID) {
                        // no child, backtrack on the tree
                        next_sid = cur_sid;
                        while (next_sid != root_sid &&
                               m_gq_vertices[next_sid].m_sid == INVALID_SID) {
                            next_sid = m_foreign_key_refs[next_sid].m_sid;
                        }
                        if (next_sid != root_sid) {
                            // there is a next child
                            next_sid = m_gq_vertices[next_sid].m_sid;
                        } // or we have reached the tree root
                    }

                    if (pass == dry_run_pass) {
                        ++m_gq_vertices[root_sid].m_n_child_tables;
                    } else {
                        m_gq_vertices[root_sid].m_child_table_sid[
                            m_gq_vertices[root_sid].m_n_child_tables++]
                            = cur_sid;
                        // We can safely fix m_root_sid here because we won't
                        // go down the tree from cur_sid for a third time.
                        //
                        // Note we may still come back to this node to move
                        // to the next node (through the horizontal link m_sid),
                        // so we can't fix that here.
                        m_gq_vertices[cur_sid].m_root_sid = root_sid;
                    }
                    cur_sid = next_sid;
                }
            }

            if (pass != dry_run_pass) {
                m_gq_vertices[root_sid].m_root_sid = root_sid;
                // Make sure we have collected the correct amount of
                // consolidated table under this tree.
                RT_ASSERT(m_gq_vertices[root_sid].m_n_child_tables ==
                        m_gq_vertices[root_sid].m_n_child_tables_copy);
            }

            // Move to the next unconsolidated table.
            root_sid = m_gq_vertices[root_sid].m_sid;
        }
    }

    // recover m_sid fields and check for a few invariants.
    for (SID sid = 0; sid < m_n_streams; ++sid) {
        m_gq_vertices[sid].m_sid = sid;
        RT_ASSERT((m_gq_vertices[sid].m_sid == m_gq_vertices[sid].m_root_sid) ?
            ((m_gq_vertices[sid].m_n_child_tables == 0) ?
             (!m_gq_vertices[sid].m_child_table_sid) :
             ((bool) m_gq_vertices[sid].m_child_table_sid)) :
            (!m_gq_vertices[sid].m_child_table_sid));
    }

    return true;
}

SID SJoinExecState::get_my_sid_in_pred_join(
    SID root_sid,
    Predicate *pred) const {
    return (m_gq_vertices[pred->get_lsid()].m_root_sid == root_sid) ?
        pred->get_lsid() : pred->get_rsid();
}

bool SJoinExecState::create_query_graph() {
    // prepare the unconsolidated tables for BFS
    UINT4 n_unconsolidated_streams = 0;
    for (SID sid = 0; sid < m_n_streams; ++sid) {
        if (m_gq_vertices[sid].m_root_sid == sid) {
            // An unconsolidated table.
            // INVALID_SID represents unvisited vertices
            m_gq_vertices[sid].m_sid = INVALID_SID;
            m_gq_vertices[sid].m_pred_join =
                m_mmgr->allocate_array<Predicate*>(m_n_streams);
            memset(m_gq_vertices[sid].m_pred_join, 0, sizeof(Predicate*) * m_n_streams);
            ++n_unconsolidated_streams;
        }
    }

    // initialize edges
    //
    // TODO not sure why this causes double free:
    // std_vector<std_vector<Predicate*>> edges(m_mmgr,
    //  m_n_streams, std_vector<Predicate*>(m_mmgr));
    std_vector<std_vector<Predicate*>> edges(m_mmgr);
    for (SID sid = 0; sid < m_n_streams; ++sid) {
        edges.emplace_back(m_mmgr);
    }
    for (Predicate *pred: m_predicates) {
        SID lsid = m_gq_vertices[pred->get_lsid()].m_root_sid;
        SID rsid = m_gq_vertices[pred->get_rsid()].m_root_sid;
        edges[lsid].push_back(pred);
        edges[rsid].push_back(pred);
    }

    // BFS
    std_deque<SID> q(m_mmgr);
    SID starting_sid = m_gq_vertices[0].m_root_sid;
    m_gq_vertices[starting_sid].m_sid = starting_sid;;
    q.push_back(starting_sid);
    SID n_visited = 0;

    while (!q.empty()) {
        ++n_visited;

        SID cur_sid = q.front();
        q.pop_front();

        for (Predicate *pred : edges[cur_sid]) {
            SID my_sid = get_my_sid_in_pred_join(cur_sid, pred);
            SID other_sid =
                m_gq_vertices[pred->get_the_other_sid(my_sid)].m_root_sid;

            if (m_gq_vertices[other_sid].m_sid == INVALID_SID) {
                ++m_gq_vertices[cur_sid].m_n_neighbors;
                m_gq_vertices[cur_sid].m_pred_join[other_sid] = pred;

                ++m_gq_vertices[other_sid].m_n_neighbors;
                m_gq_vertices[other_sid].m_pred_join[cur_sid] = pred;

                q.push_back(other_sid);
                m_gq_vertices[other_sid].m_sid = other_sid;
            }
        }
    }

    if (n_visited < n_unconsolidated_streams) {
        std::cerr << "[ERROR] some tables are not connected in the query graph"
            << std::endl;
        return false;
    }

    for (SID sid = 0; sid < m_n_streams; ++sid) {
        SJoinQueryGraphVertex *s = &m_gq_vertices[sid];
        if (s->m_root_sid != sid) continue;
        RT_ASSERT(s->m_n_neighbors > 0);
        RT_ASSERT(s->m_sid == sid);

        s->m_neighbor_sid = m_mmgr->allocate_array<SID>(s->m_n_neighbors + 1);
        SID i = 0;
        for (SID sid2 = 0; sid2 < m_n_streams; ++sid2) {
            if (s->m_pred_join[sid2]) {
                s->m_pred_join[i] = s->m_pred_join[sid2];
                ++i;
            }
        }
        std::sort(s->m_pred_join, s->m_pred_join + s->m_n_neighbors,
            [this, sid](Predicate *p1, Predicate *p2) -> bool {
                SID sid1 = get_my_sid_in_pred_join(sid, p1);
                RT_ASSERT(m_gq_vertices[sid1].m_root_sid == sid);

                SID sid2 = get_my_sid_in_pred_join(sid, p2);
                RT_ASSERT(m_gq_vertices[sid2].m_root_sid == sid);

                if (sid1 < sid2) {
                    return true;
                }
                if (sid1 > sid2) {
                    return false;
                }

                return p1->get_my_colid(sid1) < p2->get_my_colid(sid2);
            });

        // All code after this assumes SJoinQueryGraphVertex.m_pred_join
        // starts from index number 1.
        std::memmove(s->m_pred_join + 1, s->m_pred_join,
            s->m_n_neighbors * sizeof(Predicate*));

        for (i = 1; i <= s->m_n_neighbors; ++i) {
            Predicate *p = s->m_pred_join[i];
            SID sid_actual = get_my_sid_in_pred_join(sid, p);
            s->m_neighbor_sid[i] =
                m_gq_vertices[p->get_the_other_sid(sid_actual)].m_root_sid;
        }
        s->m_neighbor_sid[0] = s->m_sid;
        s->m_pred_join[0] = s->m_pred_join[1];

        // FIXME assume there's no (multi-table) filter at all
        // TODO handle filters
        s->m_n_filters = edges[sid].size() - s->m_n_neighbors;
        if (s->m_n_filters != 0) {
            std::cerr << "[ERROR] TODO multi-table fitlers are not implemented yet"
                << std::endl;
            std::cerr << "Did you specify a cyclic query? It's not implemented yet."
                << std::endl;
            return false;
        }
        RT_ASSERT(s->m_n_filters == 0);
        s->m_filters = nullptr;
    }

    return true;
}

bool SJoinExecState::create_node_layout() {
    for (SID sid = 0; sid < m_n_streams; ++sid) {
        SJoinQueryGraphVertex *s = &m_gq_vertices[sid];
        if (s->m_root_sid != s->m_sid) {
            // No node layout is needed for a consolidated table.
            continue;
        }

        PayloadOffset payload_offset = 0;
        // Indexes
        s->m_n_keys = 1;
        for (UINT4 i = 1; i <= s->m_n_neighbors; ++i) {
            SID prev_my_sid = get_my_sid_in_pred_join(
                sid, s->m_pred_join[i-1]);
            SID my_sid = get_my_sid_in_pred_join(
                sid, s->m_pred_join[i]);
            if (prev_my_sid != my_sid ||
                s->m_pred_join[i]->get_my_colid(my_sid) !=
                s->m_pred_join[i-1]->get_my_colid(prev_my_sid)) {
                ++s->m_n_keys;
            }
        }

        s->m_index_id = m_mmgr->allocate_array<IndexId>(s->m_n_keys);
        s->m_index_n_neighbors_prefix_sum = m_mmgr->allocate_array<UINT4>(s->m_n_keys + 1);
        s->m_key_sid = m_mmgr->allocate_array<COLID>(s->m_n_keys);
        s->m_key_colid = m_mmgr->allocate_array<COLID>(s->m_n_keys);
        s->m_key_ti = m_mmgr->allocate_array<typeinfo*>(s->m_n_keys);
        s->m_offset_key = m_mmgr->allocate_array<PayloadOffset>(s->m_n_keys);
        s->m_offset_hdiff = m_mmgr->allocate_array<PayloadOffset>(s->m_n_keys);
        s->m_offset_parent = m_mmgr->allocate_array<PayloadOffset>(s->m_n_keys);
        s->m_offset_left = m_mmgr->allocate_array<PayloadOffset>(s->m_n_keys);
        s->m_offset_right = m_mmgr->allocate_array<PayloadOffset>(s->m_n_keys);
        IndexId j = 0;
        for (UINT4 i = 0; i <= s->m_n_neighbors; ++i) {
            SID my_sid = get_my_sid_in_pred_join(
                sid, s->m_pred_join[i]);
            if (i > 0) {
                SID prev_my_sid = get_my_sid_in_pred_join(
                    sid, s->m_pred_join[i-1]);
                if (prev_my_sid == my_sid &&
                    s->m_pred_join[i]->get_my_colid(prev_my_sid) ==
                    s->m_pred_join[i-1]->get_my_colid(my_sid)) {
                    // A duplicate key, skip it.
                    continue;
                }
            }

            s->m_index_id[j] = m_n_indexes++;
            s->m_index_n_neighbors_prefix_sum[j] = i;
            s->m_key_sid[j] = my_sid;
            s->m_key_colid[j] = s->m_pred_join[i]->get_my_colid(my_sid);
            s->m_key_ti[j] = m_stables[my_sid]->get_schema()->get_ti(s->m_key_colid[j]);
            s->m_offset_key[j] = allocate_vertex_payload<SJoinJoinGraphVertex>(
                payload_offset,
                s->m_key_ti[j]->get_size(),
                s->m_key_ti[j]->get_alignment());
            s->m_offset_hdiff[j] = allocate_vertex_payload<SJoinJoinGraphVertex>(
                payload_offset,
                sizeof(INT4),
                sizeof(INT4));
            s->m_offset_left[j] = allocate_vertex_payload<SJoinJoinGraphVertex>(
                payload_offset,
                sizeof(SJoinJoinGraphVertex*),
                sizeof(SJoinJoinGraphVertex*));
            s->m_offset_right[j] = allocate_vertex_payload<SJoinJoinGraphVertex>(
                payload_offset,
                sizeof(SJoinJoinGraphVertex*),
                sizeof(SJoinJoinGraphVertex*));
            s->m_offset_parent[j] = allocate_vertex_payload<SJoinJoinGraphVertex>(
                payload_offset,
                sizeof(SJoinJoinGraphVertex*),
                sizeof(SJoinJoinGraphVertex*));
		    ++j;
        }
        s->m_index_n_neighbors_prefix_sum[s->m_n_keys] = s->m_n_neighbors + 1;

        // prune table look ups not needed in the record projection
        if (s->m_n_child_tables > 0) {
            s->m_child_table_needed_for_projection =
                m_mmgr->allocate_array<bool>(s->m_n_child_tables);
            for (UINT4 i = s->m_n_child_tables; i; ) {
                --i;
                SID child_table_sid = s->m_child_table_sid[i];
                if (!s->m_child_table_needed_for_projection[i]) {
                    if (std::find(s->m_key_sid,
                                s->m_key_sid + s->m_n_keys,
                                child_table_sid) !=
                            s->m_key_sid + s->m_n_keys) {
                        s->m_child_table_needed_for_projection[i] = true;
                    }
                }
                // also need to mark its non-root parent table as needed
                if (m_foreign_key_refs[child_table_sid].m_sid !=
                        s->m_sid) {
                    SID parent_table_sid =
                        m_foreign_key_refs[child_table_sid].m_sid;
                    UINT4 k = std::find(s->m_child_table_sid,
                            s->m_child_table_sid + s->m_n_child_tables,
                            parent_table_sid) - s->m_child_table_sid;
                    RT_ASSERT(k < s->m_n_child_tables);
                    s->m_child_table_needed_for_projection[k] = true;
                }
            }
        }

        // Weights & ptrs
        s->m_offset_weight =
            m_mmgr->allocate_array<PayloadOffset>(s->m_n_neighbors + 1);
        s->m_offset_subtree_weight =
            m_mmgr->allocate_array<PayloadOffset>(s->m_n_neighbors + 1);
        s->m_offset_child_sum_weight =
            m_mmgr->allocate_array<PayloadOffset>(s->m_n_neighbors + 1);
        for (IndexId i = 0; i <= s->m_n_neighbors; ++i) {
            s->m_offset_weight[i] = allocate_vertex_payload<SJoinJoinGraphVertex>(
                payload_offset,
                sizeof(WEIGHT),
                sizeof(WEIGHT));
            s->m_offset_subtree_weight[i] = allocate_vertex_payload<SJoinJoinGraphVertex>(
                payload_offset,
                sizeof(WEIGHT),
                sizeof(WEIGHT));
            if (i != 0) {
                s->m_offset_child_sum_weight[i] = allocate_vertex_payload<SJoinJoinGraphVertex>(
                    payload_offset,
                    sizeof(WEIGHT),
                    sizeof(WEIGHT));
            }
        }

        allocate_vertex_payload<SJoinJoinGraphVertex>(payload_offset, 0, 8);
        s->m_vertex_size = sizeof(SJoinJoinGraphVertex) + payload_offset;

        // to be filled in the next loop
        s->m_neighbor_idx_of_me_in_neighbor =
            m_mmgr->allocate_array<UINT4>(s->m_n_neighbors + 1);
        s->m_offset_key_indexed_by_neighbor_idx =
            m_mmgr->allocate_array<PayloadOffset>(s->m_n_neighbors + 1);
        s->m_key_sid_indexed_by_neighbor_idx =
            m_mmgr->allocate_array<SID>(s->m_n_neighbors + 1);
        s->m_key_colid_indexed_by_neighbor_idx =
            m_mmgr->allocate_array<COLID>(s->m_n_neighbors + 1);
        s->m_index_id_indexed_by_neighbor_idx =
            m_mmgr->allocate_array<IndexId>(s->m_n_neighbors + 1);
    }

    // fill in fast indexes
    for (SID sid = 0; sid < m_n_streams; ++sid) {
        SJoinQueryGraphVertex *gq_vertex = &m_gq_vertices[sid];
        if (gq_vertex->m_root_sid != gq_vertex->m_sid) {
            // Skip the consolidated tables.
            continue;
        }

        // for each key
        for (UINT4 index_idx = 0; index_idx < gq_vertex->m_n_keys; ++index_idx) {
            // for each neighbor
            for (UINT4 neighbor_idx = gq_vertex->m_index_n_neighbors_prefix_sum[index_idx];
                    neighbor_idx < gq_vertex->m_index_n_neighbors_prefix_sum[index_idx+1];
                    ++neighbor_idx) {

                // 0 is sid itself
                if (neighbor_idx != 0) {
                    // neighbor
                    SID my_sid = get_my_sid_in_pred_join(
                        sid, gq_vertex->m_pred_join[neighbor_idx]);
                    SID other_sid = m_gq_vertices[
                        gq_vertex->m_pred_join[neighbor_idx]
                        ->get_the_other_sid(my_sid)].m_root_sid;
                    SJoinQueryGraphVertex *gq_vertex2 = &m_gq_vertices[other_sid];
                    for (UINT4 j = 1; j <= gq_vertex2->m_n_neighbors; ++j) {
                        if (gq_vertex2->m_neighbor_sid[j] == sid) {
                            gq_vertex2->m_neighbor_idx_of_me_in_neighbor[j] =
                                neighbor_idx;
                            break;
                        }
                    }
                }

                gq_vertex->m_offset_key_indexed_by_neighbor_idx[neighbor_idx] =
                    gq_vertex->m_offset_key[index_idx];
                gq_vertex->m_key_sid_indexed_by_neighbor_idx[neighbor_idx] =
                    gq_vertex->m_key_sid[index_idx];
                gq_vertex->m_key_colid_indexed_by_neighbor_idx[neighbor_idx] =
                    gq_vertex->m_key_colid[index_idx];
                gq_vertex->m_index_id_indexed_by_neighbor_idx[neighbor_idx] =
                    gq_vertex->m_index_id[index_idx];
            }
        }
    }

    return true;
}

void SJoinExecState::clean_query_graph() {
    if (m_gq_vertices) {
        for (SID sid = 0; sid < m_n_streams; ++sid) {
            SJoinQueryGraphVertex *s = &m_gq_vertices[sid];
            m_mmgr->deallocate(s->m_filters);
            m_mmgr->deallocate(s->m_neighbor_sid);
            m_mmgr->deallocate(s->m_pred_join);
            m_mmgr->deallocate(s->m_child_table_sid);
            m_mmgr->deallocate(s->m_child_table_needed_for_projection);
            m_mmgr->deallocate(s->m_index_id);
            m_mmgr->deallocate(s->m_index_n_neighbors_prefix_sum);
            m_mmgr->deallocate(s->m_key_colid);
            m_mmgr->deallocate(s->m_key_ti);
            m_mmgr->deallocate(s->m_offset_key);
            m_mmgr->deallocate(s->m_offset_hdiff);
            m_mmgr->deallocate(s->m_offset_left);
            m_mmgr->deallocate(s->m_offset_right);
            m_mmgr->deallocate(s->m_offset_parent);
            m_mmgr->deallocate(s->m_offset_weight);
            m_mmgr->deallocate(s->m_offset_subtree_weight);
            m_mmgr->deallocate(s->m_offset_child_sum_weight);
            m_mmgr->deallocate(s->m_neighbor_idx_of_me_in_neighbor);
            m_mmgr->deallocate(s->m_offset_key_indexed_by_neighbor_idx);
            m_mmgr->deallocate(s->m_key_sid_indexed_by_neighbor_idx);
            m_mmgr->deallocate(s->m_key_colid_indexed_by_neighbor_idx);
            m_mmgr->deallocate(s->m_index_id_indexed_by_neighbor_idx);
        }

        m_mmgr->deallocate(m_gq_vertices);
        m_gq_vertices = nullptr;
    }
}

bool SJoinExecState::create_indexes() {
    bool has_consolidated_table =
        std::find_if(m_gq_vertices, m_gq_vertices + m_n_streams,
            [](const SJoinQueryGraphVertex &s) -> bool {
                return s.m_root_sid != s.m_sid;
            });

    // hash tables
    m_vertex_hash_table = m_mmgr->allocate_array<VertexHashTable>(m_n_streams);
    if (has_consolidated_table) {
        m_consolidated_table_tuple_hash_table =
            m_mmgr->allocate_array<ConsolidatedTableTupleHashTable>(m_n_streams);
    }
    for (SID sid = 0; sid < m_n_streams; ++sid) {
        if (m_gq_vertices[sid].m_sid == m_gq_vertices[sid].m_root_sid) {
            m_vertex_hash_table[sid].initialize(
                &m_gq_vertices[sid], m_generic_mempool);
        } else {
            m_consolidated_table_tuple_hash_table[sid].initialize(
                m_stables[sid], m_generic_mempool);
        }
    }

    // AVL tree indexes
    m_index_info = m_mmgr->allocate_array<IndexInfo>(m_n_indexes);
    for (SID sid = 0; sid < m_n_streams; ++sid) {
        SJoinQueryGraphVertex *gq_vertex = &m_gq_vertices[sid];
        if (gq_vertex->m_sid != gq_vertex->m_root_sid) continue;

        for (UINT4 i = 0; i < gq_vertex->m_n_keys; ++i) {
            create_index(gq_vertex, i);
        }
    }

    // vertex mempool
    m_vertex_mempool = m_mmgr->allocate_array<mempool_memmgr*>(m_n_streams);
    for (SID sid = 0; sid < m_n_streams; ++sid) {
        if (m_gq_vertices[sid].m_sid != m_gq_vertices[sid].m_root_sid) {
            continue;
        }

        size_t vsize = m_gq_vertices[sid].m_vertex_size;
        size_t block_size = mempool_memmgr::calc_block_size_from_block_bytes(
            1 * 1024 * 1024, vsize);
        m_vertex_mempool[sid] = create_memmgr<mempool_memmgr>(
            m_mmgr,
            vsize,
            block_size);
    }

    return true;
}

void SJoinExecState::create_index(
    SJoinQueryGraphVertex *gq_vertex,
    UINT4 i) {
    IndexId index_id = gq_vertex->m_index_id[i];

    AVLNodeDesc desc;
    desc.m_offset_key = gq_vertex->m_offset_key[i];
    desc.m_offset_left = gq_vertex->m_offset_left[i];
    desc.m_offset_right = gq_vertex->m_offset_right[i];
    desc.m_offset_parent = gq_vertex->m_offset_parent[i];
    desc.m_offset_hdiff = gq_vertex->m_offset_hdiff[i];
    desc.m_key_ti = gq_vertex->m_key_ti[i];

    m_index_info[index_id].m_base_sid = gq_vertex->m_sid;
    m_index_info[index_id].m_index = m_mmgr->newobj<AVL>(&desc);
}

void SJoinExecState::clean_indexes() {
    // vertex mempool
    if (m_vertex_mempool) {
        for (SID sid = 0; sid < m_n_streams; ++sid) {
            if (m_gq_vertices[sid].m_root_sid != m_gq_vertices[sid].m_sid) continue;
            destroy_memmgr(m_vertex_mempool[sid]);
        }
        m_mmgr->deallocate(m_vertex_mempool);
        m_vertex_mempool = nullptr;
    }

    // AVL tree indexes
    if (m_index_info) {
        for (IndexId i = 0; i < m_n_indexes; ++i) {
            m_mmgr->deallocate(m_index_info[i].m_index);
        }
        m_mmgr->deallocate(m_index_info);
        m_index_info = nullptr;
    }

    // Hash tables.
    //
    // Only need to deallocate the array because the internal
    // nodes of the has tables are allocated from
    // m_generic_mempool which will be reclaimed upon destruction.
    m_mmgr->deallocate(m_vertex_hash_table);
    m_vertex_hash_table = nullptr;
    m_mmgr->deallocate(m_consolidated_table_tuple_hash_table);
    m_consolidated_table_tuple_hash_table = nullptr;
}

void SJoinExecState::clean_all() {
    clean_sampling();

    m_mmgr->deallocate(m_vertices);
    m_vertices = nullptr;
    
    clean_indexes();

    clean_query_graph();

    m_mmgr->deallocate(m_rowids_for_projection);
    m_rowids_for_projection = nullptr;

    if (m_generic_mempool) {
        destroy_memmgr(m_generic_mempool);
        m_generic_mempool = nullptr;
    }
}

// class SJoinExecState
SJoinExecState::~SJoinExecState() {
    clean_all();
}

void SJoinExecState::reinit() {
    m_generic_mempool->deallocate_all();
    for (SID sid = 0; sid < m_n_streams; ++sid) {
        if (m_gq_vertices[sid].m_root_sid == m_gq_vertices[sid].m_sid) {
            m_vertex_mempool[sid]->deallocate_all();
            m_vertex_hash_table[sid].initialize(&m_gq_vertices[sid], m_generic_mempool);
        } else {
            m_consolidated_table_tuple_hash_table[sid].initialize(
                m_stables[sid], m_generic_mempool);
        }
    }

    memset(m_vertices, 0, sizeof(SJoinJoinGraphVertex*) * m_n_streams);
    for (IndexId i = 0; i < m_n_indexes; ++i) {
        m_index_info[i].m_index->clear();
    }

    // invalidated by deallocation
    m_join_sample.m_rowids = nullptr;

    reinit_sampling();
}

WEIGHT SJoinExecState::compute_child_sum_weight(
    SJoinQueryGraphVertex   *gq_vertex,
    SJoinJoinGraphVertex    *vertex,
    UINT4                   neighbor_idx) {

    Datum value;
    DATUM low;
    DATUM high;
    PayloadOffset offset_key =
        gq_vertex->m_offset_key_indexed_by_neighbor_idx[neighbor_idx];
    PayloadOffset offset_child_sum_weight =
        gq_vertex->m_offset_child_sum_weight[neighbor_idx];

    SID sid2 = gq_vertex->m_neighbor_sid[neighbor_idx];
    SJoinQueryGraphVertex *gq_vertex2 = &m_gq_vertices[sid2];
    UINT4 neighbor_idx_of_me = gq_vertex->m_neighbor_idx_of_me_in_neighbor[neighbor_idx];
    IndexId neighbor_index_id = gq_vertex2->m_index_id_indexed_by_neighbor_idx[neighbor_idx_of_me];
    PayloadOffset offset_neighbor_offset_subtree_weight =
        gq_vertex2->m_offset_subtree_weight[neighbor_idx_of_me];
    value = datum_at(vertex, offset_key);
    
    Predicate *pred = gq_vertex->m_pred_join[neighbor_idx];
    SID my_actual_sid = get_my_sid_in_pred_join(gq_vertex->m_sid, pred);
    pred->compute_range_ii(
        my_actual_sid,
        value,
        &low,
        &high);
    WEIGHT weight = m_index_info[neighbor_index_id].m_index->sum_range_ii(
            &low,
            &high,
            offset_neighbor_offset_subtree_weight);
    weight_at(vertex, offset_child_sum_weight) = weight;
    return weight;
}

std_vector<UpdatedVertexInfo> SJoinExecState::insert_multiple_records(
    SID sid,
    ROWID *rowid,
    UINT8 num_rows) {

    SJoinQueryGraphVertex *gq_vertex = &m_gq_vertices[sid];

    if (gq_vertex->m_root_sid == gq_vertex->m_sid) {
        for (UINT8 i = 0; i < num_rows; ++i) {
            insert_record_into_consolidated_table(
                sid,
                rowid[i]);
        }
        return std_vector<UpdatedVertexInfo>(m_generic_mempool);
    }

    /* Allocate return value vector. */
    std_vector<UpdatedVertexInfo> updated_vertex_info(m_generic_mempool);

    /* For dedupliating vertices */
    std_unordered_map<SJoinJoinGraphVertex*, UINT8> all_vertices(m_generic_mempool);

    for (UINT8 rowid_idx = 0; rowid_idx != num_rows; ++rowid_idx) {
        SJoinJoinGraphVertex *vertex = project_record(sid, rowid[rowid_idx]);
        // We have a foreign key constraint violation.
        // We have warned the user and just pretend we are inserting a
        // consolidated table's tuple.
        // TODO Should do something more meaningful. And the result could
        // be wrong as we don't update the join graph when that
        // primary key finally arrives.
        if (!vertex) continue;

        if (m_vertex_hash_table[sid].find_or_insert(vertex))
        {
            m_vertices[sid] = nullptr;
        }

        auto iter = all_vertices.find(vertex);
        if (iter == all_vertices.end()) {
            /* first touch of the vertex, add it to the result set */
            all_vertices.emplace(vertex, (UINT8) updated_vertex_info.size());
            updated_vertex_info.resize(updated_vertex_info.size() + 1);
            auto &info = updated_vertex_info.back();
            info.m_vertex = vertex;
            info.m_old_rowids_len = (UINT8) vertex->m_rowids.size();
        }

        /* append rowid to the tuple list of the vertex */
        vertex->m_rowids.push_back(rowid[rowid_idx]);
    }

    UINT8 num_updated_vertices = (UINT8) updated_vertex_info.size();

    /* update weights
     * w_0: SID is the query tree root
     * w_0 = W_1 * W_2 * ... * W_{n_neighbors} * len
     */
    for (UINT8 vertex_idx = 0; vertex_idx < num_updated_vertices; ++vertex_idx) {
        auto &info = updated_vertex_info[vertex_idx];
        WEIGHT old_w0, new_w0;
        if (info.m_old_rowids_len == 0) {
            old_w0 = 0;
            new_w0 = (WEIGHT) info.m_vertex->m_rowids.size();
            for (UINT4 neighbor_idx = 1;
                    neighbor_idx <= gq_vertex->m_n_neighbors;
                    ++neighbor_idx) {
                WEIGHT s = compute_child_sum_weight(
                        gq_vertex,
                        info.m_vertex,
                        neighbor_idx);
                new_w0 *= s;
            }
        } else {
            old_w0 = weight_at(info.m_vertex, gq_vertex->m_offset_weight[0]);
            new_w0 = old_w0 / info.m_old_rowids_len
                * (WEIGHT) info.m_vertex->m_rowids.size();
        }
        weight_at(info.m_vertex, gq_vertex->m_offset_weight[0]) = new_w0;

        // TODO do we still need to pass num_results back to the caller?
        // Not now...
        // num_results = new_w0 - old_w0;
    }

    // weight i (i >= 1): the neighbor/its ancestor is the query tree root
    std_vector<std::pair<Datum, WEIGHT>> u(m_generic_mempool);
    for (UINT4 neighbor_idx = 1;
            neighbor_idx <= gq_vertex->m_n_neighbors;
            ++neighbor_idx) {

        for (UINT8 vertex_idx = 0; vertex_idx < num_updated_vertices; ++vertex_idx) {
            auto &info = updated_vertex_info[vertex_idx];
            if (info.m_old_rowids_len == 0) {
                // new vertex, compute from scratch
                WEIGHT Wi = weight_at(info.m_vertex, gq_vertex->m_offset_child_sum_weight[neighbor_idx]);
                WEIGHT wi;

                if (Wi != 0) {
                    WEIGHT new_w0 = weight_at(info.m_vertex, gq_vertex->m_offset_weight[0]);
                    wi = new_w0 / Wi;
                } else {
                    wi = (WEIGHT) info.m_vertex->m_rowids.size() -
                        info.m_old_rowids_len;
                    for (UINT4 neighbor_idx2 = 1;
                            neighbor_idx2 <= gq_vertex->m_n_neighbors;
                            ++neighbor_idx2) {
                        if (neighbor_idx2 == neighbor_idx) continue;
                        wi *= weight_at(info.m_vertex, gq_vertex->m_offset_child_sum_weight[neighbor_idx2]);
                    }
                }
                weight_at(info.m_vertex, gq_vertex->m_offset_weight[neighbor_idx]) = wi;

                if (wi != 0) {
                    Datum my_value = datum_at(info.m_vertex,
                        gq_vertex->m_offset_key_indexed_by_neighbor_idx[neighbor_idx]);
                    u.emplace_back(my_value, wi);
                }
            } else {
                WEIGHT old_wi = weight_at(info.m_vertex, gq_vertex->m_offset_weight[neighbor_idx]);
                WEIGHT new_wi;

                new_wi = old_wi / info.m_old_rowids_len
                    * (WEIGHT) info.m_vertex->m_rowids.size();

                if (new_wi != old_wi) {
                    weight_at(info.m_vertex, gq_vertex->m_offset_weight[neighbor_idx]) = new_wi;
                    Datum my_value = datum_at(info.m_vertex,
                        gq_vertex->m_offset_key_indexed_by_neighbor_idx[neighbor_idx]);
                    u.emplace_back(my_value, new_wi - old_wi);
                }
            }
        }

        if (!u.empty()) {
            /* sort u and update the neighbor root */
            typeinfo *ti = gq_vertex->m_pred_join[neighbor_idx]->get_ti();
            std::sort(u.begin(), u.end(),
                [ti](const auto &p1, const auto &p2) -> bool {
                    return ti->compare(p1.first, p2.first) < 0;
                });
            update_neighbor(
                sid,
                gq_vertex->m_neighbor_sid[neighbor_idx],
                gq_vertex->m_neighbor_idx_of_me_in_neighbor[neighbor_idx],
                u);
            u.clear();
        }
    }

    /* Lastly, insert the vertices into the tree or fix agg. */
    for (UINT8 vertex_idx = 0; vertex_idx < num_updated_vertices; ++vertex_idx) {
        auto &info = updated_vertex_info[vertex_idx];

        if (info.m_old_rowids_len == 0) {
            // insert vertex
            for (UINT4 index_idx = 0; index_idx < gq_vertex->m_n_keys; ++index_idx) {
                AVL *index = m_index_info[gq_vertex->m_index_id[index_idx]].m_index;
                UINT4 base = gq_vertex->m_index_n_neighbors_prefix_sum[index_idx];
                index->insert(info.m_vertex,
                    gq_vertex->m_index_n_neighbors_prefix_sum[index_idx + 1] - base,
                    gq_vertex->m_offset_weight + base,
                    gq_vertex->m_offset_subtree_weight + base);
            }
        } else {
            // just fix agg
            for (UINT4 index_idx = 0; index_idx < gq_vertex->m_n_keys; ++index_idx) {
                AVL *index = m_index_info[gq_vertex->m_index_id[index_idx]].m_index;
                UINT4 base = gq_vertex->m_index_n_neighbors_prefix_sum[index_idx];
                index->fix_agg(
                    info.m_vertex,
                    gq_vertex->m_index_n_neighbors_prefix_sum[index_idx + 1] - base,
                    gq_vertex->m_offset_weight + base,
                    gq_vertex->m_offset_subtree_weight + base);
            }
        }
    }

    return updated_vertex_info;
}

SJoinJoinGraphVertex *SJoinExecState::insert_record(
    SID sid,
    ROWID rowid,
    WEIGHT *num_results) {
    SJoinQueryGraphVertex *gq_vertex = &m_gq_vertices[sid];
    
    if (gq_vertex->m_root_sid != gq_vertex->m_sid) {
        insert_record_into_consolidated_table(
            sid,
            rowid);
        *num_results = 0;
        return nullptr;
    }

    SJoinJoinGraphVertex *vertex = project_record(sid, rowid);
    // We have a foreign key constraint violation.
    // We have warned the user and just pretend we are inserting a
    // consolidated table's tuple.
    //
    // NOTE: for some tpc-ds queries, when we some primary keys
    // may not exist because they have NULL values in its other
    // join columns, in which case, it is ok (and expected) to
    // just drop this record.
    //
    // TODO Should do something more meaningful. And the result could
    // be wrong as we don't update the join graph when that
    // primary key finally arrives.
    if (!vertex) return nullptr;

    /*
     * vertex is passed as a reference of pointer to find_or_insert,
     * and the callee will update vertex to the one in the hash table.
     */
    if (m_vertex_hash_table[sid].find_or_insert(vertex)) {
        m_vertices[sid] = nullptr;
    }
    // append to tuple list
    WEIGHT old_len = (WEIGHT) vertex->m_rowids.size();
    vertex->m_rowids.push_back(rowid);

    // update weights of vertex
    // weight 0 is special: SID is the query tree root
    // w_0 = W_1 * W_2 * ... * W_{n_neighbors} * len
    WEIGHT old_w0, new_w0;
    if (old_len == 0) {
        old_w0 = 0;
        new_w0 = 1;
        for (UINT4 neighbor_idx = 1;
                neighbor_idx <= gq_vertex->m_n_neighbors;
                ++neighbor_idx) {
            WEIGHT s = compute_child_sum_weight(
                gq_vertex,
                vertex,
                neighbor_idx);
            new_w0 *= s;
        }
    } else {
        old_w0  = weight_at(vertex, gq_vertex->m_offset_weight[0]);
        new_w0 = old_w0 / old_len * (old_len + 1);
    }
    weight_at(vertex, gq_vertex->m_offset_weight[0]) = new_w0;
    // weight i (i >= 1): the neighbor/its ancestor is the query tree root
    std_vector<std::pair<Datum, WEIGHT>> u(m_generic_mempool);
    if (old_len == 0) {
        for (UINT4 neighbor_idx = 1;
                neighbor_idx <= gq_vertex->m_n_neighbors;
                ++neighbor_idx) {
            // new vertex, must compute from scratch
            WEIGHT Wi = weight_at(vertex, gq_vertex->m_offset_child_sum_weight[neighbor_idx]);
            WEIGHT wi;
            if (Wi != 0) {
                wi = new_w0 / Wi;
            } else {
                wi = 1; // old_len + 1
                for (UINT4 neighbor_idx2 = 1;
                        neighbor_idx2 <= gq_vertex->m_n_neighbors;
                        ++neighbor_idx2) {
                    if (neighbor_idx2 == neighbor_idx) continue;
                    wi *= weight_at(vertex, gq_vertex->m_offset_child_sum_weight[neighbor_idx2]);
                }
            }
            weight_at(vertex, gq_vertex->m_offset_weight[neighbor_idx]) = wi;

            if (wi != 0) {
                Datum my_value = datum_at(
                    vertex,
                    gq_vertex->m_offset_key_indexed_by_neighbor_idx[neighbor_idx]);
                u.emplace_back(my_value, wi);
                update_neighbor(
                    sid,
                    gq_vertex->m_neighbor_sid[neighbor_idx],
                    gq_vertex->m_neighbor_idx_of_me_in_neighbor[neighbor_idx],
                    u);
                u.clear();
            }
        }
    } else {
        for (UINT4 neighbor_idx = 1;
                neighbor_idx <= gq_vertex->m_n_neighbors;
                ++neighbor_idx) {
            //WEIGHT Wi = weight_at(vertex, gq_vertex->m_offset_child_sum_weight[neighbor_idx]);
            WEIGHT old_wi = weight_at(vertex, gq_vertex->m_offset_weight[neighbor_idx]);
            WEIGHT new_wi;

            //if (Wi != 0) {
            //    new_wi = new_w0 / Wi;
            //} else {
            new_wi = old_wi / old_len * (old_len + 1);
            //}

            if (new_wi != old_wi) {
                // update neighbor
                weight_at(vertex, gq_vertex->m_offset_weight[neighbor_idx]) = new_wi;
                Datum my_value = datum_at(
                        vertex,
                        gq_vertex->m_offset_key_indexed_by_neighbor_idx[neighbor_idx]);
                u.emplace_back(my_value, new_wi - old_wi);
                update_neighbor(
                    sid,
                    gq_vertex->m_neighbor_sid[neighbor_idx],
                    gq_vertex->m_neighbor_idx_of_me_in_neighbor[neighbor_idx],
                    u);
                u.clear();
            }
        }
    }

    // insert into tree or fix agg
    if (old_len == 0) {
        // insert vertex
        for (UINT4 index_idx = 0; index_idx < gq_vertex->m_n_keys; ++index_idx) {
            AVL *index = m_index_info[gq_vertex->m_index_id[index_idx]].m_index;
            UINT4 base = gq_vertex->m_index_n_neighbors_prefix_sum[index_idx];
            index->insert(
                vertex,
                gq_vertex->m_index_n_neighbors_prefix_sum[index_idx + 1] - base,
                gq_vertex->m_offset_weight + base,
                gq_vertex->m_offset_subtree_weight + base);
        }
    } else {
        // fix agg
        for (UINT4 index_idx = 0; index_idx < gq_vertex->m_n_keys; ++index_idx) {
            AVL *index = m_index_info[gq_vertex->m_index_id[index_idx]].m_index;
            UINT4 base = gq_vertex->m_index_n_neighbors_prefix_sum[index_idx];
            index->fix_agg(
                vertex,
                gq_vertex->m_index_n_neighbors_prefix_sum[index_idx + 1] - base,
                gq_vertex->m_offset_weight + base,
                gq_vertex->m_offset_subtree_weight + base);
        }
    }
    *num_results = new_w0 - old_w0;
    return vertex;
}

void SJoinExecState::insert_record_into_consolidated_table(
    SID sid,
    ROWID rowid) {
    RT_ASSERT(m_gq_vertices[sid].m_root_sid != sid);
    if (!m_consolidated_table_tuple_hash_table[sid].insert(rowid)) {
        // violation of primary key constraint
        // TODO do something more meaningful
        if (m_num_key_constraint_warnings <
            m_max_key_constraint_warnings) {
            Record rec;
            m_stables[sid]->get_record(rowid, &rec);
            COLID pkey_colid =
                m_stables[sid]->get_schema()->get_primary_key_colid();
            std::cerr << "[WARN] primary key constraint violation: "
                << "column (" << sid << ", " << pkey_colid << ") = "
                << rec.to_string(pkey_colid)
                << " already exists"
                << std::endl;
            ++m_num_key_constraint_warnings;
        }
    }
}

ROWID SJoinExecState::find_record_from_consolidated_table(
    SID sid,
    ROWID parent_rowid) {
    
    SID parent_table_sid = m_foreign_key_refs[sid].m_sid;
    RT_ASSERT(parent_rowid != INVALID_ROWID);

    Record rec;
    m_stables[parent_table_sid]->get_record(parent_rowid, &rec);
    DATUM key = rec.get_column(m_foreign_key_refs[sid].m_colid);
    ROWID rowid = m_consolidated_table_tuple_hash_table[sid].find(key);
    if (rowid == INVALID_ROWID) {
        if (m_num_key_constraint_warnings <
            m_max_key_constraint_warnings) {
            std::cerr << "[WARN] foreign key constraint violation: "
                << "column (" << parent_table_sid << ", "
                << m_foreign_key_refs[sid].m_colid << ") = ``"
                << rec.to_string(m_foreign_key_refs[sid].m_colid)
                << "'' not found in column (" << sid
                << ", "
                << m_stables[sid]->get_schema()->get_primary_key_colid()
                << ")" << std::endl;
            ++m_num_key_constraint_warnings;
        }
    }
    return rowid;
}

void SJoinExecState::remove_record_from_consolidated_table(
    SID sid,
    ROWID rowid) {
    RT_ASSERT(m_gq_vertices[sid].m_root_sid != sid);
    m_consolidated_table_tuple_hash_table[sid].remove(rowid);
}

void SJoinExecState::update_neighbor(
    SID                 s0, // parent
    SID                 s1, // me
    UINT4               neighbor_idx_of_s0, // in me
    std_vector<std::pair<Datum, WEIGHT>> &u) {

    typedef std_vector<std::pair<Datum, WEIGHT>>::size_type size_type;
    SJoinQueryGraphVertex *gq_vertex = &m_gq_vertices[s1];
    std_vector<std::pair<Datum, WEIGHT>> *u2;
    size_type                   l;
    size_type                   r;
    SJoinJoinGraphVertex        *vertex;
    Predicate                   *pred;
    PayloadOffset               offset_my_key;
    IndexId                     index_id;
    AVL                         *I;
    AVL				            *I2;
    typeinfo                    *ti;
    DATUM                       low;
    DATUM                       high;
    WEIGHT                      dW;
    UINT4                       n_updated_weights;
    IndexId			            id_of_index_to_update;
    PayloadOffset               *offset_updated_weight;
    PayloadOffset               *offset_updated_subtree_weight;
    SID                         s0_actual_sid;
    SID                         s1_actual_sid;

    pred = gq_vertex->m_pred_join[neighbor_idx_of_s0];
    offset_my_key = gq_vertex->m_offset_key_indexed_by_neighbor_idx[neighbor_idx_of_s0];
    index_id = gq_vertex->m_index_id_indexed_by_neighbor_idx[neighbor_idx_of_s0];
    I = m_index_info[index_id].m_index;
    ti = pred->get_ti();

    // The sid in the predicates may be child tables'.
    s0_actual_sid = get_my_sid_in_pred_join(s0, pred);
    s1_actual_sid = get_my_sid_in_pred_join(s1, pred);

    l = 0;
    r = 0;
    dW = 0;

    // position vertex to the first one >= low(u[l].first)
    pred->compute_low_i(s0_actual_sid, u[0].first, &low);
    vertex = I->lower_bound(&low);
    if (!vertex) return ;

    // initialize queues
    u2 = m_generic_mempool->allocate_array<std_vector<std::pair<Datum, WEIGHT>>>(
        gq_vertex->m_n_neighbors + 1);
    for (UINT4 neighbor_idx = 1;
            neighbor_idx <= gq_vertex->m_n_neighbors;
            ++neighbor_idx) {
        if (neighbor_idx != neighbor_idx_of_s0) {
            new (u2 + neighbor_idx) std_vector<std::pair<Datum, WEIGHT>>(m_generic_mempool);
        }
    }

    offset_updated_weight =
        m_generic_mempool->allocate_array<PayloadOffset>(
            gq_vertex->m_n_neighbors + 1);
    offset_updated_subtree_weight =
        m_generic_mempool->allocate_array<PayloadOffset>(
            gq_vertex->m_n_neighbors + 1);

    for (;;)  {
        Datum key = datum_at(vertex, offset_my_key);
        pred->compute_range_ii(s1_actual_sid, key, &low, &high);

        while (r < u.size() && ti->compare(u[r].first, &high) <= 0) {
            dW += u[r].second;
            ++r;
        }
        while (l < u.size() && ti->compare(u[l].first, &low) < 0) {
            dW -= u[l].second;
            ++l;
        }
        if (l == u.size()) {
            break;
        }

        if (dW != 0) {
            WEIGHT old_W_s0 = weight_at(vertex,
                    gq_vertex->m_offset_child_sum_weight[neighbor_idx_of_s0]);
            WEIGHT new_W_s0 = old_W_s0 + dW;
            weight_at(vertex, gq_vertex->m_offset_child_sum_weight[neighbor_idx_of_s0]) =
                new_W_s0;
	
	    /* we try to group the fixaggs in the same index into one single call */
            n_updated_weights = 0;
	        id_of_index_to_update = 0;

            // w_0 = W_0 * w_0
            WEIGHT new_w0 = new_W_s0 *
                weight_at(vertex, gq_vertex->m_offset_weight[neighbor_idx_of_s0]);
            if (new_w0 != weight_at(vertex, gq_vertex->m_offset_weight[0])) {
                weight_at(vertex, gq_vertex->m_offset_weight[0]) = new_w0;

                offset_updated_weight[n_updated_weights] =
                    gq_vertex->m_offset_weight[0];
                offset_updated_subtree_weight[n_updated_weights++] =
                    gq_vertex->m_offset_subtree_weight[0];

                /* id of index containing w_0 is always 0, delay until
                   we encounter the next index to fixagg on or the end */
            }

            // new_w_i = old_w_i / old_W_s0 * new_W_s0 = new_w_0 / W_i
            for (UINT4 neighbor_idx = 1;
                    neighbor_idx <= gq_vertex->m_n_neighbors;
                    ++neighbor_idx) {

                if (neighbor_idx == neighbor_idx_of_s0) continue;

                //WEIGHT W_i = weight_at(vertex,
                //    gq_vertex->m_offset_child_sum_weight[neighbor_idx]);
                WEIGHT new_w_i;
                WEIGHT old_w_i = weight_at(vertex,
                    gq_vertex->m_offset_weight[neighbor_idx]);

                //if (W_i != 0) {
                //    new_w_i = new_w0 / W_i;
                //} else {
                    if (old_W_s0 != 0) {
                        new_w_i = old_w_i / old_W_s0 * new_W_s0;
                    } else {
                        // have to compute from scratch in this case
                        new_w_i = (WEIGHT) vertex->m_rowids.size();
                        for (UINT4 neighbor_idx2 = 1;
                                neighbor_idx2 <= gq_vertex->m_n_neighbors;
                                ++neighbor_idx2) {
                            if (neighbor_idx2 != neighbor_idx) {
                                new_w_i *= weight_at(vertex,
                                    gq_vertex->m_offset_child_sum_weight[neighbor_idx2]);
                            }
                        }
                    }
                //}

                if (new_w_i != old_w_i) {
                     // Before updating the weight and record the offsets,
                     // we call the pending fixagg.
                    if (gq_vertex->m_index_id_indexed_by_neighbor_idx[neighbor_idx] !=
                        id_of_index_to_update) {
                        if (n_updated_weights) {
                            I2 = m_index_info[id_of_index_to_update].m_index;
                            I2->fix_agg(
                                 vertex,
                                 n_updated_weights,
                                 offset_updated_weight,
                                 offset_updated_subtree_weight);
                            n_updated_weights = 0;
                        }
                        id_of_index_to_update = gq_vertex->m_index_id_indexed_by_neighbor_idx[neighbor_idx];
                    }
                    weight_at(vertex, gq_vertex->m_offset_weight[neighbor_idx]) = new_w_i;

                    offset_updated_weight[n_updated_weights] =
                        gq_vertex->m_offset_weight[neighbor_idx];
                    offset_updated_subtree_weight[n_updated_weights++] =
                        gq_vertex->m_offset_subtree_weight[neighbor_idx];

                    u2[neighbor_idx].emplace_back(
                        datum_at(vertex,
                            gq_vertex->m_offset_key_indexed_by_neighbor_idx[neighbor_idx]),
                        new_w_i - old_w_i);

                }
            }

            if (n_updated_weights != 0) {
                I2 = m_index_info[id_of_index_to_update].m_index;
                I2->fix_agg(
                    vertex,
                    n_updated_weights,
                    offset_updated_weight,
                    offset_updated_subtree_weight);
            }
        }

        // check range
        if (l == r) {
            // reposition vertex
            pred->compute_low_i(s0_actual_sid, u[l].first, &low);
            vertex = I->lower_bound(&low);
        } else {
            vertex = I->next(vertex);
        }

        if (!vertex) break;
    }

    // sort u2 and update neighbors
    for (UINT4 neighbor_idx = 1;
            neighbor_idx <= gq_vertex->m_n_neighbors;
            ++neighbor_idx) {

        if (neighbor_idx == neighbor_idx_of_s0) continue;

        if (!u2[neighbor_idx].empty()) {
            if (gq_vertex->m_key_sid_indexed_by_neighbor_idx[neighbor_idx] !=
                gq_vertex->m_key_sid_indexed_by_neighbor_idx[neighbor_idx_of_s0] ||
                gq_vertex->m_key_colid_indexed_by_neighbor_idx[neighbor_idx] !=
                gq_vertex->m_key_colid_indexed_by_neighbor_idx[neighbor_idx_of_s0]) {
                // A different column than the join key with s0 which is not sorted.
                // Hence, we need to sort it by he join key with the neighbor.
                typeinfo *ti_for_this_column = gq_vertex->m_pred_join[neighbor_idx]->get_ti();
                std::sort(u2[neighbor_idx].begin(), u2[neighbor_idx].end(),
                [ti_for_this_column](const auto &p1, const auto &p2) -> bool {
                    return ti_for_this_column->compare(p1.first, p2.first) < 0;
                });
            }

            update_neighbor(
                s1,
                gq_vertex->m_neighbor_sid[neighbor_idx],
                gq_vertex->m_neighbor_idx_of_me_in_neighbor[neighbor_idx],
                u2[neighbor_idx]);
        }

        u2[neighbor_idx].~std_vector<std::pair<Datum, WEIGHT>>();
    }

    // deallocate queues
    m_generic_mempool->deallocate(offset_updated_subtree_weight);
    m_generic_mempool->deallocate(offset_updated_weight);
    m_generic_mempool->deallocate(u2);
}

SJoinJoinGraphVertex *SJoinExecState::project_record(
    SID sid,
    ROWID rowid) {

    Record rec;
    SJoinQueryGraphVertex *gq_vertex = &m_gq_vertices[sid];
    SJoinJoinGraphVertex *vertex;

    if (!m_vertices[sid]) {
        m_vertices[sid] = (SJoinJoinGraphVertex *)
            m_vertex_mempool[sid]->allocate(gq_vertex->m_vertex_size);
        new (&m_vertices[sid]->m_rowids) std_deque<ROWID>(m_generic_mempool);
    }
    vertex = m_vertices[sid];
    
    // populate the rowids of child tables
    DARG0(
        std::fill(m_rowids_for_projection,
            m_rowids_for_projection + m_n_streams,
            INVALID_ROWID);
    )
    m_rowids_for_projection[sid] = rowid;
    for (UINT4 i = 0; i < gq_vertex->m_n_child_tables; ++i) {
        if (!gq_vertex->m_child_table_needed_for_projection[i]) {
            continue;
        }
        SID child_table_sid = gq_vertex->m_child_table_sid[i];
        m_rowids_for_projection[child_table_sid] = 
            find_record_from_consolidated_table(
                child_table_sid,
                m_rowids_for_projection[
                    m_foreign_key_refs[child_table_sid].m_sid]);
        if (m_rowids_for_projection[child_table_sid] == INVALID_ROWID) {
            // foreign key constraint violation
            return nullptr;
        }
    }

    for (IndexId i = 0; i < gq_vertex->m_n_keys; ++i) {
        SID key_sid = gq_vertex->m_key_sid[i];
        m_stables[key_sid]->get_record(
            m_rowids_for_projection[key_sid],
            &rec);
        rec.copy_column(
            datum_at(vertex, gq_vertex->m_offset_key[i]),
            gq_vertex->m_key_colid[i]);
    }

    return vertex;
}

void SJoinExecState::remove_multiple_records(
    SID sid,
    ROWID start_rowid,
    UINT8 num_rows) {

    SJoinQueryGraphVertex *gq_vertex = &m_gq_vertices[sid];
    if (gq_vertex->m_root_sid != sid) {
        // Consolidated table case.
        for (UINT8 rowid_idx = 0; rowid_idx < num_rows; ++rowid_idx) {
            remove_record_from_consolidated_table(
                sid, start_rowid + rowid_idx);
        }
        return ;
    }

    /* all updated vertices */
    std_vector<UpdatedVertexInfo> updated_vertex_info(m_generic_mempool);

    /* For deduplicating vertices */
    std_unordered_map<SJoinJoinGraphVertex*, UINT8> all_vertices(m_generic_mempool);

    /* find corresponding vertices and remove rowids from them */
    for (UINT8 rowid_idx = 0; rowid_idx < num_rows; ++rowid_idx) {
        ROWID rowid = start_rowid + rowid_idx;
        SJoinJoinGraphVertex *vertex = project_record(sid, rowid);
        if (!vertex) {
            // We have a foreign key constraint violation.
            // This is bad because we are no longer able to project this
            // record to a vertex, i.e., we can't remove it.
            //
            // TODO Should do something more meaningful.
            // And the result could be wrong as we don't update the
            // join graph.
            continue;
        }
        auto hash_table_iter = m_vertex_hash_table[sid].m_index.find(vertex);
        RT_ASSERT(hash_table_iter != m_vertex_hash_table[sid].m_index.end(),
                "vertex for (%hu, %lu) not found", sid, rowid);
        vertex = *hash_table_iter;

        auto iter = all_vertices.find(vertex);
        if (iter == all_vertices.end()) {
            all_vertices.emplace(vertex, (UINT8) updated_vertex_info.size());
            updated_vertex_info.resize(updated_vertex_info.size() + 1);
            auto &info = updated_vertex_info.back();
            info.m_vertex = vertex;
            info.m_old_rowids_len = (UINT8) vertex->m_rowids.size();
        } else {
            /* Got it in the updated vertex set. */
        }
        
        // TODO allow out-of-order deletion for non-sliding-window
        // scenario
        RT_ASSERT(!vertex->m_rowids.empty()
                && vertex->m_rowids.front() == rowid,
                "expired tuples are not in the same order as arrival");
        vertex->m_rowids.pop_front();
    }

    UINT8 num_updated_vertices = (UINT8) updated_vertex_info.size();

    /* update the neighbors first */
    std_vector<std::pair<Datum, WEIGHT>> u(m_generic_mempool);
    for (UINT4 neighbor_idx = 1;
            neighbor_idx <= gq_vertex->m_n_neighbors;
            ++neighbor_idx) {
        for (UINT8 vertex_idx = 0; vertex_idx < num_updated_vertices; ++vertex_idx) {
            auto &info = updated_vertex_info[vertex_idx];

            WEIGHT old_wi = weight_at(info.m_vertex, gq_vertex->m_offset_weight[neighbor_idx]);
            WEIGHT new_wi = old_wi / info.m_old_rowids_len
                * info.m_vertex->m_rowids.size();

            if (new_wi != old_wi) {
                weight_at(info.m_vertex, gq_vertex->m_offset_weight[neighbor_idx]) = new_wi;

                Datum my_value = datum_at(
                        info.m_vertex,
                        gq_vertex->m_offset_key_indexed_by_neighbor_idx[neighbor_idx]);
                u.emplace_back(my_value, new_wi - old_wi);
            }
        }

        if (!u.empty()) {
            typeinfo *ti = gq_vertex->m_pred_join[neighbor_idx]->get_ti();

            std::sort(u.begin(), u.end(),
                [ti](const auto &p1, const auto &p2) -> bool {
                    return ti->compare(p1.first, p2.first) < 0;
                });
            update_neighbor(
                sid,
                gq_vertex->m_neighbor_sid[neighbor_idx],
                gq_vertex->m_neighbor_idx_of_me_in_neighbor[neighbor_idx],
                u);
            u.clear();
        }
    }

    /* update w0 and fix agg or remove vertex from indexes */
    for (UINT8 vertex_idx = 0; vertex_idx < num_updated_vertices; ++vertex_idx) {
        auto &info = updated_vertex_info[vertex_idx];

        if (!info.m_vertex->m_rowids.empty()) {
            WEIGHT old_w0 = weight_at(info.m_vertex, gq_vertex->m_offset_weight[0]);
            WEIGHT new_w0 = old_w0 / info.m_old_rowids_len
                * info.m_vertex->m_rowids.size();
            weight_at(info.m_vertex, gq_vertex->m_offset_weight[0]) = new_w0;

            m_num_seen -= old_w0 - new_w0;

            // fix agg
            for (UINT4 index_idx = 0; index_idx < gq_vertex->m_n_keys; ++index_idx) {
                AVL *index = m_index_info[gq_vertex->m_index_id[index_idx]].m_index;
                UINT4 base = gq_vertex->m_index_n_neighbors_prefix_sum[index_idx];
                index->fix_agg(
                    info.m_vertex,
                    gq_vertex->m_index_n_neighbors_prefix_sum[index_idx + 1] - base,
                    gq_vertex->m_offset_weight + base,
                    gq_vertex->m_offset_subtree_weight + base);
            }
        } else {
            WEIGHT old_w0 = weight_at(info.m_vertex, gq_vertex->m_offset_weight[0]);
            m_num_seen -= old_w0;

            for (UINT4 index_idx = 0; index_idx < gq_vertex->m_n_keys; ++index_idx) {
                AVL *index = m_index_info[gq_vertex->m_index_id[index_idx]].m_index;
                UINT4 base = gq_vertex->m_index_n_neighbors_prefix_sum[index_idx];
                index->erase(
                    info.m_vertex,
                    gq_vertex->m_index_n_neighbors_prefix_sum[index_idx + 1] - base,
                    gq_vertex->m_offset_weight + base,
                    gq_vertex->m_offset_subtree_weight + base);
            }

            m_vertex_hash_table[sid].m_index.erase(info.m_vertex);

            info.m_vertex->m_rowids.~std_deque<ROWID>();
            m_vertex_mempool[sid]->deallocate(info.m_vertex);
        }
    }
}

void SJoinExecState::remove_record(
    SID sid,
    ROWID rowid) {

    SJoinQueryGraphVertex *gq_vertex = &m_gq_vertices[sid];
    if (gq_vertex->m_root_sid != sid) {
        // Removing a tuple from a consolidated table.
        remove_record_from_consolidated_table(sid, rowid); 
        return ;
    }

    SJoinJoinGraphVertex *vertex = project_record(sid, rowid);
    if (!vertex) {
        // We have a foreign key constraint violation.
        // This is bad because we are no longer able to project this
        // record to a vertex, i.e., we can't remove it.
        //
        // TODO Should do something more meaningful. And the result could
        // be wrong as we don't update the join graph.
        return ;
    }

    auto hash_table_iter = m_vertex_hash_table[sid].m_index.find(vertex);
    RT_ASSERT(hash_table_iter != m_vertex_hash_table[sid].m_index.end(),
            "vertex for (%hu, %lu) not found", sid, rowid);
    vertex = *hash_table_iter;
    //DARG0(bool found = )m_vertex_hash_table[sid].find(vertex);
    //RT_ASSERT(found, "vertex for (%hu, %lu) not found", sid, rowid);
    
    // TODO allow vertices being deleted out of order in the future
    RT_ASSERT(!vertex->m_rowids.empty() && vertex->m_rowids.front() == rowid,
            "expired tuples are not in the same order as arrival");
    WEIGHT old_len = (WEIGHT) vertex->m_rowids.size();

    std_vector<std::pair<Datum, WEIGHT>> u(m_generic_mempool);
    if (old_len > 1) {
        // remove the list head
        vertex->m_rowids.pop_front();

        // update weights and update neighbors
        WEIGHT old_w0 = weight_at(vertex, gq_vertex->m_offset_weight[0]);
        WEIGHT d_w0 = old_w0 / old_len;
        WEIGHT new_w0 = old_w0 - d_w0;
        weight_at(vertex, gq_vertex->m_offset_weight[0]) = new_w0;

        //m_num_expired += d_w0;
        m_num_seen -= d_w0;

        for (UINT4 neighbor_idx = 1;
                neighbor_idx <= gq_vertex->m_n_neighbors;
                ++neighbor_idx) {

            WEIGHT old_wi = weight_at(vertex, gq_vertex->m_offset_weight[neighbor_idx]);
            WEIGHT new_wi = old_wi / old_len * (old_len - 1);

            if (new_wi != old_wi) {
                weight_at(vertex, gq_vertex->m_offset_weight[neighbor_idx]) = new_wi;
                Datum my_value = datum_at(
                    vertex,
                    gq_vertex->m_offset_key_indexed_by_neighbor_idx[neighbor_idx]);
                u.emplace_back(my_value, new_wi - old_wi);
                update_neighbor(
                    sid,
                    gq_vertex->m_neighbor_sid[neighbor_idx],
                    gq_vertex->m_neighbor_idx_of_me_in_neighbor[neighbor_idx],
                    u);
                u.clear();
            }
        }

        // fix aggs
        for (UINT4 index_idx = 0; index_idx < gq_vertex->m_n_keys; ++index_idx) {
            AVL *index = m_index_info[gq_vertex->m_index_id[index_idx]].m_index;
            UINT4 base = gq_vertex->m_index_n_neighbors_prefix_sum[index_idx];
            index->fix_agg(
                vertex,
                gq_vertex->m_index_n_neighbors_prefix_sum[index_idx + 1] - base,
                gq_vertex->m_offset_weight + base,
                gq_vertex->m_offset_subtree_weight + base);
        }
    } else {
        RT_ASSERT(old_len == 1);
        // weights are all 0 now

        WEIGHT old_w0 = weight_at(vertex, gq_vertex->m_offset_weight[0]);
        //m_num_expired += old_w0;
        m_num_seen -= old_w0;

        // update neighbors
        for (UINT4 neighbor_idx = 1;
                neighbor_idx <= gq_vertex->m_n_neighbors;
                ++neighbor_idx) {

            WEIGHT old_wi = weight_at(vertex, gq_vertex->m_offset_weight[neighbor_idx]);
            if (old_wi != 0) {
                Datum my_value = datum_at(
                    vertex,
                    gq_vertex->m_offset_key_indexed_by_neighbor_idx[neighbor_idx]);
                u.emplace_back(my_value, -old_wi);
                update_neighbor(
                    sid,
                    gq_vertex->m_neighbor_sid[neighbor_idx],
                    gq_vertex->m_neighbor_idx_of_me_in_neighbor[neighbor_idx],
                    u);
                u.clear();
            }
        }

        // remove vertex
        for (UINT4 index_idx = 0; index_idx < gq_vertex->m_n_keys; ++index_idx) {
            AVL *index = m_index_info[gq_vertex->m_index_id[index_idx]].m_index;
            UINT4 base = gq_vertex->m_index_n_neighbors_prefix_sum[index_idx];
            index->erase(
                vertex,
                gq_vertex->m_index_n_neighbors_prefix_sum[index_idx + 1] - base,
                gq_vertex->m_offset_weight + base,
                gq_vertex->m_offset_subtree_weight + base);
        }

        // remove the vertex from the hash table
        m_vertex_hash_table[sid].m_index.erase(hash_table_iter);

        // deallocate the vertex
        vertex->m_rowids.~std_deque<ROWID>();
        m_vertex_mempool[sid]->deallocate(vertex);
    }
}

void SJoinExecState::notify(SID sid, ROWID rowid) {
    WEIGHT num_results;

    SJoinJoinGraphVertex *vertex = insert_record(
            sid,
            rowid,
            &num_results);

    if (!vertex) {
        // Insertion into a consolidated table does not yield new join results.
        return ;
    }

    if (m_sampling_param.m_sample_size > 0) {
        WEIGHT start_join_number = (vertex->m_rowids.size() - 1) * num_results;
        WEIGHT end_join_number = start_join_number + num_results;
        sample_next_batch(sid, vertex, start_join_number, end_join_number);
    }
}

void SJoinExecState::expire(SID sid, ROWID rowid) {
    remove_record(sid, rowid);
}

void SJoinExecState::notify_multiple(SID sid, ROWID *rowid, UINT8 num_rows) {
    std_vector<UpdatedVertexInfo> updated_vertex_info =
        insert_multiple_records(sid, rowid, num_rows);

    if (updated_vertex_info.empty()) {
        // Insertion into a consolidated table does not yield new join results.
        return ;
    }

    if (m_sampling_param.m_sample_size > 0) {
        UINT8 num_updated_vertices = (UINT8) updated_vertex_info.size();
        SJoinQueryGraphVertex *gq_vertex = &m_gq_vertices[sid];
        for (UINT8 vertex_idx = 0; vertex_idx < num_updated_vertices; ++vertex_idx) {
            auto &info = updated_vertex_info[vertex_idx];
            WEIGHT end_join_number = weight_at(info.m_vertex, gq_vertex->m_offset_weight[0]);
            WEIGHT start_join_number = end_join_number / info.m_vertex->m_rowids.size() * info.m_old_rowids_len;
            sample_next_batch(
                    sid,
                    info.m_vertex,
                    start_join_number,
                    end_join_number);
        }
    }
}

void SJoinExecState::expire_multiple(SID sid, ROWID start_rowid, UINT8 num_rows) {
    remove_multiple_records(sid, start_rowid, num_rows);
}

void SJoinExecState::set_min_ts(TIMESTAMP min_ts) {
    RT_ASSERT(!m_window_non_overlapping && !m_is_unwindowed);

    switch (m_sampling_type) {
    case BERNOULLI_SAMPLING:
        bernoulli_set_min_ts(min_ts);
        break;

    case SAMPLING_WITH_REPLACEMENT:
        sampling_with_replacement_set_min_ts(min_ts);
        break;

    case SAMPLING_WITHOUT_REPLACEMENT:
        sampling_without_replacement_set_min_ts(min_ts);
        break;

    default:
        RT_ASSERT(false);
    }
}

void SJoinExecState::expire_all() {
    reinit();
}

// For TPC-DS pop_front support
void SJoinExecState::expire_multiple_and_retire_samples(SID sid,
        const ROWID start_rowid, UINT8 num_rows) {

    remove_multiple_records(sid, start_rowid, num_rows);

    switch (m_sampling_type) {
    case BERNOULLI_SAMPLING:
        bernoulli_retire_samples(sid, start_rowid, num_rows);
        break;
    case SAMPLING_WITH_REPLACEMENT:
        sampling_with_replacement_retire_samples(sid, start_rowid, num_rows);
        break;
    case SAMPLING_WITHOUT_REPLACEMENT:
        sampling_without_replacement_retire_samples(sid, start_rowid, num_rows);
        break;
    default:
        RT_ASSERT(false);
    }
}

bool SJoinExecState::initialize_sampling() {
    switch (m_sampling_type) {
    case BERNOULLI_SAMPLING:
        bernoulli_initialize();
        break;

    case SAMPLING_WITH_REPLACEMENT:
        sampling_with_replacement_initialize();
        break;

    case SAMPLING_WITHOUT_REPLACEMENT:
        sampling_without_replacement_initialize();
        break;

    default:
        return false;
    }

    m_num_seen = 0;

    return true;
}

void SJoinExecState::reinit_sampling() {
    m_num_seen = 0;

    switch (m_sampling_type) {
    case BERNOULLI_SAMPLING:
        bernoulli_reinit();
        break;

    case SAMPLING_WITH_REPLACEMENT:
        sampling_with_replacement_reinit();
        break;

    case SAMPLING_WITHOUT_REPLACEMENT:
        sampling_without_replacement_reinit();
        break;

    default:
        RT_ASSERT(false);
    }
}

void SJoinExecState::clean_sampling() {
    switch (m_sampling_type) {
    case BERNOULLI_SAMPLING:
        bernoulli_clean();
        break;

    case SAMPLING_WITH_REPLACEMENT:
        sampling_with_replacement_clean();
        break;

    case SAMPLING_WITHOUT_REPLACEMENT:
        sampling_without_replacement_clean();
        break;

    default:
        RT_ASSERT(false);
    }
}

void SJoinExecState::draw_random_sample(ROWID *rowids) {
    // Start from an arbitrary unconsolidated table.
    SID sid = m_gq_vertices[0].m_root_sid;
    SJoinQueryGraphVertex *gq_vertex = &m_gq_vertices[sid];

    // Go down an arbitrary index.
    IndexId index_id = gq_vertex->m_index_id[0];
    AVL *I = m_index_info[index_id].m_index;

    WEIGHT num_join_results =
        I->get_total_sum(gq_vertex->m_offset_subtree_weight[0]);
    RT_ASSERT(num_join_results > 0);

    std::uniform_int_distribution<WEIGHT> unif(0, num_join_results - 1);
    WEIGHT join_number = unif(m_rng);

    SJoinJoinGraphVertex *vertex =
        I->get_nth(
            join_number,
            gq_vertex->m_offset_weight[0],
            gq_vertex->m_offset_subtree_weight[0]);
    RT_ASSERT(vertex, "vertex not found for join number %ld", join_number);

    get_sample_by_join_number(
        INVALID_SID,
        sid,
        vertex,
        0u,
        join_number,
        rowids);
}

void SJoinExecState::get_sample_by_join_number(
    SID                     s0, // parent
    SID                     s1, // me
    SJoinJoinGraphVertex    *vertex, // parent vertex
    UINT4                   neighbor_idx_of_s0,
    WEIGHT                  join_number,
    ROWID                   *rowids) {
    
    RT_ASSERT(m_gq_vertices[s1].m_root_sid == s1,
        "expecting an unconsolidated table");
    SJoinQueryGraphVertex *gq_vertex = &m_gq_vertices[s1];

    // get rowid in s1
    WEIGHT vertex_weight =
        weight_at(vertex, gq_vertex->m_offset_weight[neighbor_idx_of_s0]);
    RT_ASSERT(join_number < vertex_weight);
    WEIGHT num_rows = (WEIGHT) vertex->m_rowids.size();
    WEIGHT tuple_weight = vertex_weight / num_rows;
    WEIGHT l = join_number / tuple_weight;
    rowids[s1] = vertex->m_rowids[l];
    join_number = join_number % tuple_weight;

    // look up rowids for child tables
    for (UINT4 i = 0; i < gq_vertex->m_n_child_tables; ++i) {
        SID child_table_sid = gq_vertex->m_child_table_sid[i];
        rowids[child_table_sid] =
            find_record_from_consolidated_table(
                child_table_sid,
                rowids[m_foreign_key_refs[child_table_sid].m_sid]);
    }

    // visit neighbors
    for (UINT4 neighbor_idx = 1;
            neighbor_idx <= gq_vertex->m_n_neighbors;
            ++neighbor_idx) {

        SID s2 = gq_vertex->m_neighbor_sid[neighbor_idx];
        if (s2 == s0) continue;

        WEIGHT Wi = weight_at(vertex, gq_vertex->m_offset_child_sum_weight[neighbor_idx]);
        WEIGHT neighbor_join_number = join_number % Wi;
        join_number /= Wi;

        SJoinQueryGraphVertex *gq_vertex2 = &m_gq_vertices[s2];
        UINT4 neighbor_idx2 = gq_vertex->m_neighbor_idx_of_me_in_neighbor[neighbor_idx];
        PayloadOffset offset_weight2 = gq_vertex2->m_offset_weight[neighbor_idx2];
        PayloadOffset offset_subtree_weight2 =
            gq_vertex2->m_offset_subtree_weight[neighbor_idx2];
        IndexId index_id2 =
            gq_vertex2->m_index_id_indexed_by_neighbor_idx[neighbor_idx2];
        AVL *I2 = m_index_info[index_id2].m_index;

        Datum my_value = datum_at(vertex,
                gq_vertex->m_offset_key_indexed_by_neighbor_idx[neighbor_idx]);
        DATUM low;
        Predicate *pred = gq_vertex->m_pred_join[neighbor_idx];
        SID s1_actual_sid = get_my_sid_in_pred_join(s1, pred);
        pred->compute_low_i(s1_actual_sid, my_value, &low);

        SJoinJoinGraphVertex *vertex2 =
            I2->get_nth_from_lower_bound(
                &low,
                neighbor_join_number,
                offset_weight2,
                offset_subtree_weight2);
        RT_ASSERT(vertex2, "vertex not found for join number %ld", join_number);

        get_sample_by_join_number(
            s1,
            s2,
            vertex2,
            gq_vertex->m_neighbor_idx_of_me_in_neighbor[neighbor_idx],
            neighbor_join_number,
            rowids);
    }
}

void SJoinExecState::sample_next_batch(
    SID                     sid,
    SJoinJoinGraphVertex    *vertex,
    WEIGHT                  start_join_number,
    WEIGHT                  end_join_number) {

    if (start_join_number == end_join_number) return ;

    // Bug fix: adjust both join number and number of results
    // by adding the weight before insertion
    while (start_join_number < end_join_number) {
        WEIGHT num_skipped = std::min(end_join_number - start_join_number, (WEIGHT) m_skip_number);
        RT_ASSERT(num_skipped >= 0);
        m_skip_number -= num_skipped;
        start_join_number += num_skipped;
        m_num_seen += num_skipped;
        m_num_processed += num_skipped;

        if (start_join_number < end_join_number) {
            if (!m_join_sample.m_rowids) {
                m_join_sample.m_rowids =
                    m_generic_mempool->allocate_array<ROWID>(m_n_streams);
            }
            get_sample_by_join_number(
                INVALID_SID,
                sid,
                vertex,
                0u,
                start_join_number,
                m_join_sample.m_rowids);
            compute_join_sample_timestamp(&m_join_sample);
            update_sample_reservoir();

            ++start_join_number;
            ++m_num_seen;
            ++m_num_processed;
            m_skip_number = get_next_skip_number();
        } else break;
    }
}

void SJoinExecState::compute_join_sample_timestamp(JoinResult *jr) {
    // unwindowed tables aren't required to have timestamp columns
    if (m_is_unwindowed) return ;
    TIMESTAMP min_ts = TS_INFTY;
    Record rec;
    for (SID sid = 0; sid < m_n_streams; ++sid) {
        m_stables[sid]->get_record(jr->m_rowids[sid], &rec);
        TIMESTAMP ts = rec.get_ts();
        if (ts < min_ts) min_ts = ts;
    }

    jr->m_ts = min_ts;
}

//
// Bernoulli sampling
//
bool SJoinExecState::bernoulli_initialize() {
    m_reservoir.reserve(262144); // 4 MB = 16 B * 262144

    DOUBLE p = m_sampling_param.m_sampling_rate;
    UINT4 N;
    if (p < 1.0 / max_walker_N) {
        N = max_walker_N;
    } else {
        N = (UINT4)(1.0 / p);
    }
    DOUBLE *probs = m_mmgr->allocate_array<DOUBLE>(N + 1);
    probs[0] = p;
    probs[N] = 1 - p;
    for (UINT4 i = 1; i < N; ++i) {
        probs[i] = probs[i-1] * (1 - p);
        probs[N] *= 1 - p;
    }
    m_walker_param = walker_construct(m_mmgr, probs, N + 1);
    m_mmgr->deallocate(probs);

    bernoulli_reinit();

    return true;
}

void SJoinExecState::bernoulli_reinit() {
    m_reservoir.clear();
    m_skip_number = bernoulli_get_next_skip_number();
}

WEIGHT SJoinExecState::bernoulli_get_next_skip_number() {
    WEIGHT s = 0;
    for (;;) {
        UINT4 ds = walker_sample(m_walker_param, m_rng);
        s += ds;
        if (ds + 1 < m_walker_param->N) break;
    }
    return s;
}

void SJoinExecState::bernoulli_update_sample_reservoir() {
    // min heap with ts
    m_reservoir.push_back(m_join_sample);
    if (!m_window_non_overlapping && !m_is_unwindowed) {
        std::push_heap(
            m_reservoir.begin(),
            m_reservoir.end(),
            std::greater<JoinResult>());
    }
    m_join_sample.m_rowids = nullptr;
}

void SJoinExecState::bernoulli_set_min_ts(TIMESTAMP min_ts) {
    while (!m_reservoir.empty() && m_reservoir.front().m_ts < min_ts) {
        m_generic_mempool->deallocate(m_reservoir.front().m_rowids);
        std::pop_heap(
            m_reservoir.begin(),
            m_reservoir.end(),
            std::greater<JoinResult>());
        m_reservoir.pop_back();
    }
}

void SJoinExecState::bernoulli_retire_samples(
    SID sid, ROWID start_rowid, UINT8 num_rows) {

    /* partition on the TID of SID in the samples with the border value of
     * start_rows + num_rows */
    ROWID min_rowid = start_rowid + num_rows;
    auto new_end = std::partition(
            m_reservoir.begin(),
            m_reservoir.end(),
            [&](const JoinResult &jr) -> bool {
                return jr.m_rowids[sid] >= min_rowid;
            });
    for (auto iter = new_end; iter != m_reservoir.end(); ++iter) {
        m_generic_mempool->deallocate(iter->m_rowids);
    }
    m_reservoir.erase(new_end, m_reservoir.end());
}

void SJoinExecState::bernoulli_clean() {
    if (m_walker_param) {
        m_walker_param->~WalkerParam();
        m_mmgr->deallocate(m_walker_param);
        m_walker_param = nullptr;
    }
}

bool SJoinExecState::sampling_without_replacement_initialize() {
    // index starts from 1
    m_reservoir.reserve(m_sampling_param.m_sample_size + 1);

    m_unif_sample_size = std::uniform_int_distribution<UINT8>(
        1, m_sampling_param.m_sample_size);

    /* we still need this for de-duplication in the unwindowed case */
    if (!m_window_non_overlapping) {
        m_sample_set.max_load_factor(0.85);
        m_sample_set.reserve(m_sampling_param.m_sample_size);

        if (m_is_unwindowed) {
            m_rowid2sample_idx = m_mmgr->allocate_array<
                std_multiset<std::pair<ROWID, UINT8>>>(m_n_streams);
            for (SID sid = 0; sid < m_n_streams; ++sid) {
                ::new(static_cast<void*>(&m_rowid2sample_idx[sid])) std_multiset<std::pair<ROWID, UINT8>>(m_mmgr);
            }
        }
    }

    m_algo_Z_threshold = m_sampling_param.m_sample_size * m_algo_Z_T;

    sampling_without_replacement_reinit();

    return true;
}

void SJoinExecState::sampling_without_replacement_reinit() {
    m_skip_number = sampling_without_replacement_get_next_skip_number();
    m_reservoir.resize(1);
}

WEIGHT SJoinExecState::vitter_algo_Z(
    UINT8 num_seen,
    UINT8 sample_size,
    UINT8 algo_Z_threshold) {

    if (num_seen < sample_size) {
        //std::cout << "num_seen < sample_size s = 0" << std::endl;
        return 0;
    }
    if (num_seen <= algo_Z_threshold) {
        // algorithm X
        // t == num_seen;  n == sample_size

        DOUBLE y = m_algo_Z_unif(m_rng);
        if (y == 0) {
            // highly unlikely to reach here
            // in which case, skip all the remaining tuples
            //std::cout << "num_seen < threshold s = MAX_WEIGHT" << std::endl;
            return MAX_WEIGHT;
        }
        WEIGHT s = 0;

        UINT8 a = num_seen + 1 - sample_size;
        UINT8 b = num_seen + 1;
        DOUBLE f = ((DOUBLE) a) / b;
        while (f > y) {
            ++s;
            f *= ((DOUBLE) (a + s))/(b + s);
        }
        //std::cout << "num_seen < threshold s = " << s << std::endl;
        return s;
    }

    // algorithm Z
    DOUBLE u, v;
    DOUBLE x;
    WEIGHT s;
    DOUBLE t1, t2;

    DOUBLE a = num_seen - sample_size + 1; // t - n + 1
    DOUBLE b = a / num_seen; // (t - n + 1) / t
    DOUBLE c = (1 + num_seen) / a; // (t + 1) / (t - n + 1)
    DOUBLE c_sqr = c * c; // c^2
    DOUBLE frac_1_n = 1.0 / sample_size;
    DOUBLE minus_frac_1_n = -1.0 / sample_size;
    do {
        v = m_algo_Z_unif(m_rng);
        x = num_seen * (pow(v, minus_frac_1_n) - 1);

        s = (WEIGHT)floor(x);
        u = m_algo_Z_unif(m_rng);

        // left hand side of formula (6.3)
        // (u * c^2 * (t - n + 1 + s) / (t + x))^(1/n)
        t1 = pow(u * c_sqr * ((a + s) / (num_seen + x)) , frac_1_n);

        // right hand side of formula (6.3)
        // (t - n + 1)(t + x) / t(t - n + 1 + s)
        t2 = b * ((num_seen + x) / (a + s));

        // (6.3)
        if (t1 <= t2) break;

        // compute f(s) / cg(x)
        t1 = pow((num_seen + x) / num_seen, sample_size) / c;
        if (s < (WEIGHT) sample_size) { // s + 1 <= n
        // (t+x)/(t-n) * ((t+x)/t)^n / c * (t-n)^\overline{s+1}/(t+1)^\overline{s+1}
            t1 *= (num_seen + x) / (num_seen - sample_size);
            for (WEIGHT i = 0; i <= s; ++i) {
                t1 *= (num_seen - sample_size + i)
                    / (num_seen + 1.0 + i);
            }
        } else { // s + 1 > n
        // or
        // (t+x)/(t+s+1) * ((t+x)/t)^n / c* (t)^\underline{n}/(t+s)^\underline{n}
            t1 *= (num_seen + x) / (num_seen + 1 + s);
            for (UINT8 i = 0; i < sample_size; ++i) {
                t1 *= (num_seen - i) / (num_seen + s - (DOUBLE) i);
            }
        }
    } while (u > t1);

    //std::cout << "num_seen >= threshold s = " << s << std::endl;
    return s;
}

// inlined
//WEIGHT SJoinExecState::sampling_without_replacement_get_next_skip_number() {
    //return vitter_algo_Z(m_num_seen, m_sampling_param.m_sample_size);
//}

void SJoinExecState::sampling_without_replacement_update_sample_reservoir() {
    UINT8 idx;
    if (m_reservoir.size() <= m_sampling_param.m_sample_size) {
        m_reservoir.push_back(m_join_sample);
        if (!m_window_non_overlapping && !m_is_unwindowed) {
            std::push_heap(
                m_reservoir.begin() + 1,
                m_reservoir.end(),
                std::greater<JoinResult>());
        }
        idx = m_reservoir.size() - 1;
    } else {
        idx = m_unif_sample_size(m_rng);
        ROWID *old_rowids = m_reservoir[idx].m_rowids;

        if (!m_window_non_overlapping) {
            DARG0(auto n_removed = ) m_sample_set.erase(old_rowids);
            RT_ASSERT(n_removed == 1);

            if (!m_is_unwindowed) {
                TIMESTAMP ts = m_join_sample.m_ts;
                if (ts < m_reservoir[idx].m_ts) {
                    // push up
                    UINT8 idx2 = idx >> 1;
                    while (idx2 > 0) {
                        if (m_reservoir[idx2].m_ts > ts) {
                            m_reservoir[idx] = m_reservoir[idx2];
                            idx = idx2;
                            idx2 = idx >> 1;
                        } else break;
                    }
                } else if (ts > m_reservoir[idx].m_ts) {
                    // push down
                    UINT2 idx2 = idx << 1;
                    while (idx2 <= m_sampling_param.m_sample_size) {
                        if (idx2 < m_sampling_param.m_sample_size &&
                            m_reservoir[idx2 + 1].m_ts < m_reservoir[idx2].m_ts) {
                            ++idx2;
                        }
                        if (m_reservoir[idx2].m_ts < ts) {
                            m_reservoir[idx] = m_reservoir[idx2];
                            idx = idx2;
                            idx2 = idx << 1;
                        } else break;
                    }
                }
            } else {
                for (SID sid = 0; sid < m_n_streams; ++sid) {
                    m_rowid2sample_idx[sid].erase(
                        std::make_pair(m_join_sample.m_rowids[sid], idx));
                }
            }
        }
        m_generic_mempool->deallocate(old_rowids);
        m_reservoir[idx] = m_join_sample;
    }
    if (!m_window_non_overlapping) {
        DARG0(auto p = )m_sample_set.insert(m_join_sample.m_rowids);
        RT_ASSERT(p.second);

        if (m_is_unwindowed) {
            /* for tpc-ds deletion case */
            for (SID sid = 0; sid < m_n_streams; ++sid) {
                m_rowid2sample_idx[sid].emplace(m_join_sample.m_rowids[sid], idx);
            }
        }
    }
    m_join_sample.m_rowids = nullptr;
}

void SJoinExecState::sampling_without_replacement_set_min_ts(TIMESTAMP min_ts) {
    while (m_reservoir.size() > 1 &&
            m_reservoir[1].m_ts < min_ts) {
        std::pop_heap(
            m_reservoir.begin() + 1,
            m_reservoir.end(),
            std::greater<JoinResult>());

        DARG0(auto n_removed = ) m_sample_set.erase(m_reservoir.back().m_rowids);
        RT_ASSERT(n_removed == 1);
        m_generic_mempool->deallocate(m_reservoir.back().m_rowids);
        m_reservoir.pop_back();
    }

    UINT8 target_num_samples = std::min(m_num_seen, m_sampling_param.m_sample_size);

    // number of samples < std::min(m_num_seen, m_sampling_param.m_sample_size)
    if (m_reservoir.size() <= target_num_samples) {
        if (m_num_seen <= 2 * m_sampling_param.m_sample_size) {
            // just iterate through everything again
            sampling_without_replacement_rebuild_reservoir();
        } else {
            // rejection sampling
            do {
                if (!m_join_sample.m_rowids) {
                    m_join_sample.m_rowids =
                        m_generic_mempool->allocate_array<ROWID>(m_n_streams);
                }
                decltype(m_sample_set.end()) iter;
                DARG0(int i = 0);
                do {
                    draw_random_sample(m_join_sample.m_rowids);
                    iter = m_sample_set.find(m_join_sample.m_rowids);
                    DARG0(++i);
                } while (iter != m_sample_set.end());
                compute_join_sample_timestamp(&m_join_sample);
                sampling_without_replacement_update_sample_reservoir();
            } while (m_reservoir.size() <= target_num_samples);
        }
    }

    m_skip_number = sampling_without_replacement_get_next_skip_number();

    RT_ASSERT(m_reservoir.size() == m_sampling_param.m_sample_size + 1 ||
            m_skip_number == 0);
}

void SJoinExecState::sampling_without_replacement_rebuild_reservoir() {
    for (ROWID *rowid: m_sample_set) {
        m_generic_mempool->deallocate(rowid);
    }
    m_sample_set.clear();
    m_reservoir.resize(1);

    SID sid = 0;
    SJoinQueryGraphVertex *gq_vertex = &m_gq_vertices[sid];
    IndexId index_id = gq_vertex->m_index_id[0];
    AVL *I = m_index_info[index_id].m_index;

    RT_ASSERT(
        I->get_total_sum(gq_vertex->m_offset_subtree_weight[0]) ==
        (WEIGHT) m_num_seen);

    UINT8 t = 0;
    UINT8 n = m_sampling_param.m_sample_size;

    while (t < m_num_seen) {
        WEIGHT join_number = (WEIGHT) t;
        SJoinJoinGraphVertex *vertex =
            I->get_nth(
                join_number,
                gq_vertex->m_offset_weight[0],
                gq_vertex->m_offset_subtree_weight[0]);
        WEIGHT n_results =
            weight_at(vertex, gq_vertex->m_offset_weight[0]);
        while (join_number < n_results) {
            if (t < n ||
                m_algo_Z_unif(m_rng) < ((DOUBLE) n) / (t + 1)) {

                if (!m_join_sample.m_rowids) {
                    m_join_sample.m_rowids =
                        m_generic_mempool->allocate_array<ROWID>(m_n_streams);
                }
                get_sample_by_join_number(
                    INVALID_SID,
                    sid,
                    vertex,
                    0u,
                    join_number,
                    m_join_sample.m_rowids);
                compute_join_sample_timestamp(&m_join_sample);
                sampling_without_replacement_update_sample_reservoir();
            }
            ++join_number;
            ++t;
        }
    }
}

void SJoinExecState::sampling_without_replacement_retire_samples(
    SID sid, ROWID start_rowid, UINT8 num_rows) {

    UINT8 min_rowid = start_rowid + num_rows;
    std_multiset<std::pair<ROWID, UINT8>> &rowid2sample_idx = m_rowid2sample_idx[sid];
    decltype(rowid2sample_idx.begin()) new_begin;
    UINT8 num_deleted = 0;
    for (new_begin = rowid2sample_idx.begin();
        new_begin->first < min_rowid; ++new_begin) {
        ++num_deleted;
    }

    if (num_deleted) {
        if (m_num_seen <= 2 * m_sampling_param.m_sample_size) {
            // just iterate through everything again
            for (SID sid = 0; sid < m_n_streams; ++sid) {
                m_rowid2sample_idx[sid].clear();
            }
            sampling_without_replacement_rebuild_reservoir();
        } else {
            // erase all the expired samples first
            UINT8 *deleted_idx = m_generic_mempool->allocate_array<UINT8>(num_deleted);
            UINT8 i = 0;
            for (auto iter = rowid2sample_idx.begin();
                    iter != new_begin;) {
                UINT8 idx = iter->second;
                deleted_idx[i++] = idx;

                DARG0(auto n_removed = ) m_sample_set.erase(m_reservoir[idx].m_rowids);
                RT_ASSERT(n_removed == 1);
                // don't deallocate rowid array yet: they will be reused very soon
                for (SID sid2 = 0; sid2 < m_n_streams; ++sid2) {
                    if (sid2 != sid) {
                        m_rowid2sample_idx[sid2].erase(
                            std::make_pair(m_reservoir[idx].m_rowids[sid], idx));
                    } else {
                        rowid2sample_idx.erase(iter++);
                    }
                }
            }
            RT_ASSERT(i == num_deleted);

            // rejection sampling

            for (UINT8 i = 0; i < num_deleted; ++i) {
                UINT8 idx = deleted_idx[i];

                decltype(m_sample_set.end()) iter;
                DARG0(UINT8 num_samples_actually_fetched = 0);
                do {
                    draw_random_sample(m_reservoir[idx].m_rowids);
                    DARG0(++num_samples_actually_fetched);
                } while (iter != m_sample_set.end());
                compute_join_sample_timestamp(&m_reservoir[idx]);

                DARG0(auto p =) m_sample_set.insert(m_reservoir[idx].m_rowids);
                RT_ASSERT(p.second);

                for (SID sid =  0; sid < m_n_streams; ++sid) {
                    m_rowid2sample_idx[sid].emplace(m_reservoir[idx].m_rowids[sid], idx);
                }
            }
        }
    }
}

void SJoinExecState::sampling_without_replacement_clean() {
    // rowids will be deallocated with the generic mempool

    if (m_rowid2sample_idx) {
        for (SID sid = 0; sid < m_n_streams; ++sid) {
            (&m_rowid2sample_idx[sid])->~std_multiset<std::pair<ROWID, UINT8>>();
        }
        m_mmgr->deallocate(m_rowid2sample_idx);
        m_rowid2sample_idx = nullptr;
    }
}


// sampling with replacement

bool SJoinExecState::sampling_with_replacement_initialize() {
    // index starts from 1
    m_reservoir.resize(m_sampling_param.m_sample_size);
    for (UINT8 i = 0; i < m_sampling_param.m_sample_size; ++i) {
        m_reservoir[i].m_rowids = m_mmgr->allocate_array<ROWID>(m_n_streams);
    }

    m_min_heap_nproc.resize(m_sampling_param.m_sample_size + 1);
    for (UINT8 i = 0; i < m_sampling_param.m_sample_size; ++i) {
        m_min_heap_nproc[i+1].m_sample_idx = i;
    }

    if (!m_window_non_overlapping && !m_is_unwindowed) {
        m_min_heap_ts.resize(m_sampling_param.m_sample_size + 1);
        m_sample_idx2min_heap_ts_idx.resize(m_sampling_param.m_sample_size);
        for (UINT8 i = 1; i <= m_sampling_param.m_sample_size; ++i) {
            m_min_heap_ts[i] = &m_reservoir[i - 1];
        }
    }

    if (!m_window_non_overlapping && m_is_unwindowed) {
        m_rowid2sample_idx = m_mmgr->allocate_array<
            std_multiset<std::pair<ROWID, UINT8>>>(m_n_streams);
        for (SID sid = 0; sid < m_n_streams; ++sid) {
            ::new(static_cast<void*>(&m_rowid2sample_idx[sid])) std_multiset<std::pair<ROWID, UINT8>>(m_mmgr);
        }
    }

    sampling_with_replacement_reinit();

    return true;
}

void SJoinExecState::sampling_with_replacement_reinit() {
    // the first one is never skipped
    m_skip_number = 0;
}

// inlined
//WEIGHT SJoinExecState::sampling_with_replacement_get_next_skip_number() {
//    return ((WEIGHT)(m_min_heap_nproc[1].m_next_num_processed)) - num_processed();
//}

void SJoinExecState::sampling_with_replacement_update_sample_reservoir() {
    UINT8 t = num_seen();
    UINT8 n = m_sampling_param.m_sample_size;
    UINT8 nproc = num_processed();
    if (t == 0) {
        for (UINT8 i = 1; i <= n; ++i) {
            m_reservoir[i-1].m_ts = m_join_sample.m_ts;
            memcpy(
                m_reservoir[i-1].m_rowids,
                m_join_sample.m_rowids,
                sizeof(ROWID) * m_n_streams);
            m_min_heap_nproc[i].m_next_num_processed =
                nproc + 1 + vitter_algo_Z(1, 1, m_algo_Z_T);
        }
        make_min_heap(
            m_min_heap_nproc,
            n,
            [](const SampleSkipNumber& s) -> UINT8 {
                return s.m_next_num_processed;
            });

        if (!m_window_non_overlapping && !m_is_unwindowed) {
            make_min_heap_with_inverted_index(
                m_min_heap_ts,
                m_sample_idx2min_heap_ts_idx,
                n,
                [](const JoinResult *jr) -> TIMESTAMP {
                    return jr->m_ts;
                },
                [this](const JoinResult *jr) -> UINT8 {
                    return (UINT8)(jr - m_reservoir.data());
                });
        }

        if (!m_window_non_overlapping && m_is_unwindowed) {
            for (UINT8 i = 1; i <= n; ++i) {
                for (SID sid = 0; sid < m_n_streams; ++sid) {
                    m_rowid2sample_idx[sid].emplace(m_join_sample.m_rowids[sid], i);
                }
            }
        }
    } else {
        RT_ASSERT(m_min_heap_nproc[1].m_next_num_processed == nproc);
        while (m_min_heap_nproc[1].m_next_num_processed == nproc) {
            UINT8 idx = m_min_heap_nproc[1].m_sample_idx;

            if (!m_window_non_overlapping && m_is_unwindowed) {
                for (SID sid = 0; sid < m_n_streams; ++sid) {
                    m_rowid2sample_idx[sid].erase(
                        std::make_pair(m_reservoir[idx].m_rowids[sid], idx));
                    m_rowid2sample_idx[sid].emplace(
                        m_join_sample.m_rowids[sid], idx);
                    m_reservoir[idx].m_rowids[sid] = m_join_sample.m_rowids[sid];
                }
            } else {
                memcpy(
                    m_reservoir[idx].m_rowids,
                    m_join_sample.m_rowids,
                    sizeof(ROWID) * m_n_streams);
            }
            m_min_heap_nproc[1].m_next_num_processed =
                nproc + 1 + vitter_algo_Z(t + 1, 1, m_algo_Z_T);
            push_down_min_heap(
                m_min_heap_nproc,
                1,
                n,
                [](const SampleSkipNumber& s) -> UINT8 {
                    return s.m_next_num_processed;
                });


            if (!m_window_non_overlapping && !m_is_unwindowed) {
                UINT8 min_heap_ts_idx = m_sample_idx2min_heap_ts_idx[idx];
                TIMESTAMP old_ts = m_reservoir[idx].m_ts;
                m_reservoir[idx].m_ts = m_join_sample.m_ts;
                if (m_join_sample.m_ts < old_ts) {
                    push_up_min_heap_with_inverted_index(
                        m_min_heap_ts,
                        m_sample_idx2min_heap_ts_idx,
                        min_heap_ts_idx,
                        n,
                        [](const JoinResult *jr) -> TIMESTAMP {
                            return jr->m_ts;
                        },
                        [this](const JoinResult *jr) -> UINT8 {
                            return (UINT8)(jr - m_reservoir.data());
                        });
                } else if (m_join_sample.m_ts > old_ts) {
                    push_down_min_heap_with_inverted_index(
                        m_min_heap_ts,
                        m_sample_idx2min_heap_ts_idx,
                        min_heap_ts_idx,
                        n,
                        [](const JoinResult *jr) -> TIMESTAMP {
                            return jr->m_ts;
                        },
                        [this](const JoinResult *jr) -> UINT8 {
                            return (UINT8)(jr - m_reservoir.data());
                        });
                }
            } else {
                m_reservoir[idx].m_ts = m_join_sample.m_ts;
            }
        }
    }
}

void SJoinExecState::sampling_with_replacement_set_min_ts(TIMESTAMP min_ts) {
    if (num_seen() == 0) {
        // everything has expired
        m_skip_number = 0;
        return ;
    }

    UINT8 n = m_sampling_param.m_sample_size;
    while (m_min_heap_ts[1]->m_ts < min_ts) {
        draw_random_sample(m_min_heap_ts[1]->m_rowids);
        compute_join_sample_timestamp(m_min_heap_ts[1]);
        push_down_min_heap_with_inverted_index(
            m_min_heap_ts,
            m_sample_idx2min_heap_ts_idx,
            1,
            n,
            [](const JoinResult *jr) -> TIMESTAMP {
                return jr->m_ts;
            },
            [this](const JoinResult *jr) -> UINT8 {
                return (UINT8)(jr - m_reservoir.data());
            });
    }

    UINT8 t = num_seen();
    UINT8 nproc = num_processed();
    for (UINT8 i = 1; i <= n; ++i) {
        m_min_heap_nproc[i].m_next_num_processed =
            nproc + vitter_algo_Z(t, 1, m_algo_Z_T);
    }
    make_min_heap(
        m_min_heap_nproc,
        n,
        [](const SampleSkipNumber& s) -> UINT8 {
            return s.m_next_num_processed;
        });
    m_skip_number = sampling_with_replacement_get_next_skip_number();
}

void SJoinExecState::sampling_with_replacement_retire_samples(
    SID sid, ROWID start_rowid, UINT8 num_rows) {

    if (num_seen() == 0) {
        /* everything has expired */
        m_skip_number = 0;
        for (SID sid2 = 0; sid2 < m_n_streams; ++sid2) {
            m_rowid2sample_idx[sid2].clear();
        }
        return ;
    }

    UINT8 min_rowid = start_rowid + num_rows;
    std_multiset<std::pair<ROWID, UINT8>> &rowid2sample_idx = m_rowid2sample_idx[sid];
    for (auto iter = rowid2sample_idx.begin();
            iter->first < min_rowid; ++iter) {
        UINT8 idx = iter->second;

        for (SID sid2 = 0; sid2 < m_n_streams; ++sid2) {
            if (sid == sid2) {
                rowid2sample_idx.erase(iter++);
            } else {
                m_rowid2sample_idx[sid2].erase(
                    std::make_pair(m_reservoir[idx].m_rowids[sid2], idx));
            }
        }

        draw_random_sample(m_reservoir[idx].m_rowids);
        compute_join_sample_timestamp(&m_reservoir[idx]);

        for (SID sid2 = 0; sid2 < m_n_streams; ++sid2) {
            m_rowid2sample_idx[sid2].emplace(m_reservoir[idx].m_rowids[sid2], idx);
        }
    }

    UINT8 t = num_seen();
    UINT8 nproc = num_processed();
    for (UINT8 i = 1; i <= m_sampling_param.m_sample_size; ++i) {
        m_min_heap_nproc[i].m_next_num_processed =
            nproc + vitter_algo_Z(t, 1, m_algo_Z_T);
    }
    make_min_heap(
        m_min_heap_nproc,
        m_sampling_param.m_sample_size,
        [](const SampleSkipNumber& s) -> UINT8 {
            return s.m_next_num_processed;
        });
    m_skip_number = sampling_with_replacement_get_next_skip_number();
}

void SJoinExecState::sampling_with_replacement_clean() {
    if (m_rowid2sample_idx) {
        for (SID sid = 0; sid < m_n_streams; ++sid) {
            (&m_rowid2sample_idx[sid])->~std_multiset<std::pair<ROWID, UINT8>>();
        }
        m_mmgr->deallocate(m_rowid2sample_idx);
        m_rowid2sample_idx = nullptr;
    }
}


void SJoinExecState::init_join_result_iterator() {
    m_reservoir_iter = m_reservoir.begin();
}

bool SJoinExecState::has_next_join_result() {
    return m_reservoir_iter != m_reservoir.end();
}

const JoinResult &SJoinExecState::next_join_result() {
    return *(m_reservoir_iter++);
}

void VertexHashTable::initialize(SJoinQueryGraphVertex *gq_vertex, memmgr *mmgr) {
    m_sid = gq_vertex->m_sid;
    new (&m_index) HashTable(mmgr, 1024,
            VertexHash(gq_vertex),
            VertexEqual(gq_vertex));
}

bool VertexHashTable::find_or_insert(SJoinJoinGraphVertex *&vertex) {
    auto p = m_index.insert(vertex);
    vertex = *p.first;
    return p.second;
}

void ConsolidatedTableTupleHashTable::initialize(
    STable *stable,
    memmgr *mmgr) {

    m_stable = stable;
    const Schema *schema = m_stable->get_schema();
    m_pkey_colid = schema->get_primary_key_colid();
    m_rec.set_schema(schema);
    typeinfo *ti = schema->get_ti(m_pkey_colid);

    new (&m_index) HashTable(mmgr, 1024,
            ConsolidatedTableTupleHash(ti),
            ConsolidatedTableTupleEqual(ti));
}

bool ConsolidatedTableTupleHashTable::insert(ROWID rowid) {
    BYTE *buf = m_stable->get_record_buffer(rowid);
    m_rec.set_buf(buf);
    DATUM key = m_rec.get_column(m_pkey_colid);
    auto p = m_index.emplace(key, rowid);
    return p.second;
}

ROWID ConsolidatedTableTupleHashTable::find(const DATUM &key) {
    auto iter = m_index.find(key); 
    if (iter == m_index.end()) {
        return INVALID_ROWID;
    }
    return iter->second;
}

void ConsolidatedTableTupleHashTable::remove(ROWID rowid) {
    BYTE *buf = m_stable->get_record_buffer(rowid);
    m_rec.set_buf(buf);
    DATUM key = m_rec.get_column(m_pkey_colid);
    m_index.erase(key);
}

