#ifndef QUERY_H
#define QUERY_H

#include <config.h>
#include <basictypes.h>
#include <dtypes.h>
#include <schema.h>
#include <record.h>
#include <memmgr.h>
#include <join.h>
#include <stable.h>
#include <sjoin.h>
#include <sinljoin.h>
#include <filter.h>
#include <string>
#include <cstdlib>
#include <thread>
#include <chrono>

#include <iostream>
#include <unordered_map>

class Query {
public:
    Query(
       memmgr *mmgr,
       Join *join);

    virtual ~Query();

    SID num_streams() const { return m_stables.size(); }

    SID add_stream(Schema *schema);
    
    // lsid.lcolid == lrid.rcolid
    void add_equi_join(
        SID lsid,
        COLID lcolid,
        SID rsid,
        COLID rcolid,
        Predicate::ForeignKeySpec fkey_spec = Predicate::NO_FKEY) {
        m_join->add_equi_join(lsid, lcolid, rsid, rcolid, fkey_spec);
    }

    // |lsid.lcolid - rsid.rcolid| <= datum
    void add_band_join(
        SID lsid,
        COLID lcolid,
        SID rsid,
        COLID rcolid,
        Datum datum) {
        m_join->add_band_join(lsid, lcolid, rsid, rcolid, datum);
    }

    virtual void set_bernoulli_sampling(DOUBLE sampling_rate) = 0;
    
    virtual void set_sampling_with_replacement(ROWID sample_size) = 0;

    virtual void set_sampling_without_replacement(ROWID sample_size) = 0;

    void set_tumbling_window(
        TIMESTAMP window_size);

    bool is_tumbling_window() const {
        return m_window_type == WT_TUMBLING_WINDOW;
    }

    void set_sliding_window(
        TIMESTAMP window_size);

    bool is_sliding_window() const {
        return m_window_type == WT_SLIDING_WINDOW;
    }

    void set_unwindowed(
        bool has_deletion);

    bool is_unwindowed() const {
        return m_window_type == WT_UNWINDOWED;
    }

    bool allow_deletion() const {
        // see Query::set_unwindowed() for explanation
        return !m_join->is_window_non_overlapping();
    }

    void set_no_dumping() {
        m_is_no_dumping = true;
    }

    bool is_no_dumping() const {
        return m_is_no_dumping;
    }

    // If not set, the default one will be called.
    void set_report_event_handler(
        std::function<void(
            std::ostream& /* result_out */,
            TIMESTAMP /* min_ts */,
            TIMESTAMP /* max_ts */,
            UINT8 /* num_processed_since_last_report */,
            UINT8 /* num_tuples_since_last_report */)> f) {
        m_report_event_handler = f;
    }
     
    virtual void execute(
        std::string input_filename,
        std::string result_filename,
        std::string meter_filename,
        UINT8 seed = 19950810ull) = 0;
    
    /*
     * TODO update this function to match the current impl.
     * Currently this is probably broken.
     *
     * Try to batch update up to batch_size of rows per stream. Up to
     * (batch_size - 1) * #tables + 1 rows may be pooled before any
     * stream is notified.
     */
    virtual void execute_minibatch(
        std::string input_filename,
        std::string result_filename,
        std::string meter_filename,
        UINT8 batch_size,
        UINT8 seed = 19950810ull) = 0;

    void add_equi_filter(
        SID sid,
        COLID colid,
        typeinfo *ti,
        Datum value);

    void add_band_filter(
        SID sid,
        COLID colid,
        typeinfo *ti,
        Datum operand,
        Datum value);

    void add_neq_filter(
        SID sid,
        COLID colid,
        typeinfo *ti,
        Datum value);
    
    void allow_key_constraint_warnings(
        UINT4 max_key_constraint_warnings) {
        m_max_key_constraint_warnings = max_key_constraint_warnings;
    }

    void suppress_key_constraint_warnings() {
        allow_key_constraint_warnings(0);
    }

    virtual std::pair<
        std_vector<JoinResult>::const_iterator,
        std_vector<JoinResult>::const_iterator> 
    get_samples() const = 0;

    virtual UINT8 num_seen() const = 0;

protected:
    void handle_report_event(
        std::ostream &result_out,
        TIMESTAMP min_ts,
        TIMESTAMP max_ts,
        UINT8 num_processed_since_last_report,
        UINT8 num_tuples_since_last_report);

    memmgr                          *m_mmgr;
    
    std_vector<mempool_memmgr*>     m_rec_mempool;

    std_vector<STable*>             m_stables;

    std_vector<size_t>              m_rec_max_len;

    std_vector<COLID>               m_ts_colid;
    
    Join                            *m_join;

    JoinExecState                   *m_exec_state;

    enum WindowType {
        WT_NOT_SET = 0,
        WT_UNWINDOWED,
        WT_TUMBLING_WINDOW,
        WT_SLIDING_WINDOW
    }                               m_window_type;

    TIMESTAMP                       m_window_size;

    UINT8                           m_num_tuples;

    std_vector<Filter>              m_filters;

    bool                            m_is_no_dumping;

    UINT4                           m_max_key_constraint_warnings;
    
    // A user-specified hook for report event handling.
    // May be empty.
    std::function<void(
        std::ostream& /* result_out */,
        TIMESTAMP /* min_ts */,
        TIMESTAMP /* max_ts */,
        UINT8 /* num_processed_since_last_report */,
        UINT8 /* num_tuples_since_lsat_report */)>
                                    m_report_event_handler;

    typedef decltype(std::chrono::system_clock::now()) time_point;

public:
    void start_meter(std::string meter_filename);

    void stop_meter();

private:
    UINT8 read_num_tuples() const {
        return *(const volatile UINT8*) &m_num_tuples;
    }

    void run_meter(std::string meter_filename);

    std::thread                     m_meter_thread; 
    
    volatile bool                   m_meter_stopped;

    time_point                      m_start_time;

    time_point                      m_end_time;
};

class SJoinQuery: public Query {
public:
    SJoinQuery(
        memmgr *mmgr, bool measure_update_only)
    : Query(
        mmgr,
        mmgr->newobj<SJoin>(mmgr)),
        m_sjoin(dynamic_cast<SJoin*>(m_join)),
        m_sjoin_exec_state(nullptr), measure_update_only(measure_update_only) {}

    void set_bernoulli_sampling(DOUBLE sampling_rate) override {
        m_sjoin->set_bernoulli_sampling(sampling_rate);
    }

    void set_sampling_with_replacement(ROWID sample_size) override {
        m_sjoin->set_sampling_with_replacement(sample_size);
    }

    void set_sampling_without_replacement(ROWID sample_size) override {
        m_sjoin->set_sampling_without_replacement(sample_size);
    }
    
    virtual void execute(
        std::string input_filename,
        std::string result_filename,
        std::string meter_filename,
        UINT8 seed = 19950810ull);
    
    /*
     * See Join::execute_minibatch().
     */
    virtual void execute_minibatch(
        std::string input_filename,
        std::string result_filename,
        std::string meter_filename,
        UINT8 batch_size,
        UINT8 seed = 19950810ull);

    inline std::pair<
        std_vector<JoinResult>::const_iterator,
        std_vector<JoinResult>::const_iterator> 
    get_samples() const override {
        return m_sjoin_exec_state->get_samples();
    }

    inline UINT8 num_seen() const override {
        return m_sjoin_exec_state->num_seen();
    }

protected:
    SJoin                           *m_sjoin;

    SJoinExecState                  *m_sjoin_exec_state;

    // added for measuring update time
    bool                            measure_update_only;
    time_point                      tuple_start_time;
    time_point                      tuple_end_time;
    unsigned long                   total_update_time = 0;
    std::vector<unsigned long>      update_times;
};

class SINLJoinQuery: public Query {
public:
    SINLJoinQuery(
        memmgr *mmgr)
    : Query(
        mmgr,
        mmgr->newobj<SINLJoin>(mmgr)),
      m_sinljoin(dynamic_cast<SINLJoin*>(m_join)),
      m_sinljoin_exec_state(nullptr) {}

    virtual void execute(
        std::string input_filename,
        std::string result_filename,
        std::string meter_filename,
        UINT8 seed = 19950810ull);

protected:
    virtual void process_next_join_result_during_expiration(const JoinResult &jr) = 0;
    
    virtual void process_next_join_result(const JoinResult &jr) = 0;

    virtual void handle_tumbling_window_expiration() = 0;

    virtual void handle_sliding_window_expiration(TIMESTAMP min_ts) = 0;

    virtual void handle_deletion(SID sid, ROWID start_rowid, UINT8 num_rows) = 0;

    SINLJoin                        *m_sinljoin;

    SINLJoinExecState               *m_sinljoin_exec_state;
};

class SINLJoinSamplingQuery: public SINLJoinQuery {
public:
    SINLJoinSamplingQuery(memmgr *mmgr);

    ~SINLJoinSamplingQuery();
    
    void set_bernoulli_sampling(DOUBLE sampling_rate) override {
        m_sampling_type = 'B';
        m_sampling_param.m_sampling_rate = sampling_rate;
    }

    void set_sampling_with_replacement(ROWID sample_size) override {
        m_sampling_type = 'W';
        m_sampling_param.m_sample_size = sample_size;
    }

    void set_sampling_without_replacement(ROWID sample_size) override {
        m_sampling_type = 'O';
        m_sampling_param.m_sample_size = sample_size;
    }

    virtual void execute(
        std::string input_filename,
        std::string result_filename,
        std::string meter_filename,
        UINT8 seed = 19950810ull);
    
    /*
     * TODO Not implemented yet.
     */
    virtual void execute_minibatch(
        std::string input_filename,
        std::string result_filename,
        std::string meter_filename,
        UINT8 batch_size,
        UINT8 seed = 19950810ull) {

        execute(input_filename, result_filename, meter_filename, seed);
    }

    virtual void process_next_join_result(const JoinResult &jr);

    virtual void process_next_join_result_during_expiration(const JoinResult &jr);
    
    virtual void handle_tumbling_window_expiration();

    virtual void handle_sliding_window_expiration(TIMESTAMP min_ts);

    virtual void handle_deletion(SID sid, ROWID start_rowid, UINT8 num_rows);

    inline std::pair<
        std_vector<JoinResult>::const_iterator,
        std_vector<JoinResult>::const_iterator> 
    get_samples() const override {
        return std::make_pair(m_samples.begin(), m_samples.end());
    }

    inline UINT8 num_seen() const override {
        return m_sinljoin_exec_state->num_seen(); 
    }

private:
    inline JoinResult copy_join_result(const JoinResult &jr) {
        JoinResult jr2;
        jr2.m_ts = jr.m_ts;
        jr2.m_rowids = m_generic_mempool->allocate_array<ROWID>(num_streams());
        memcpy(jr2.m_rowids, jr.m_rowids, sizeof(ROWID) * num_streams());
        return jr2;
    }

protected:
    std_vector<JoinResult>          m_samples;

    char                            m_sampling_type;

    union {
        DOUBLE                      m_sampling_rate;
        ROWID                       m_sample_size;
    }                               m_sampling_param;

    /*
     * This is where all the sample results are kept.
     */
    generic_mempool_memmgr          *m_generic_mempool;

    std::mt19937_64                 m_rng;

    std::uniform_real_distribution<DOUBLE>
                                    m_unif_0_1;

    std::uniform_int_distribution<UINT8>
                                    m_unif_sample_size;
};

#endif

