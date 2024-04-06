#include <config.h>
#include <dtypes.h>
#include <memmgr.h>
#include <lroad_queries.h>
#include <tpc_ds_queries.h>
#include <utils.h>

#include <iostream>
#include <signal.h>
#include <cstdlib>
#include <unistd.h>
#include <getopt.h>
#include <errno.h>
using namespace std;

static constexpr char getopt_optstring[] =
    "-:q:a:i:o:m:w:t:s:r:ndh::";
static constexpr struct option getopt_options[] = {
    {"query", 1, nullptr, 'q'},
    {"algorithm", 1, nullptr, 'a'},
    {"input", 1, nullptr, 'i'},
    {"output", 1, nullptr, 'o'},
    {"meter", 1, nullptr, 'm'},
    {"window-type", 1, nullptr, 'w'},
    {"window-size", 1, nullptr, 't'},
    {"sampling-type", 1, nullptr, 's'},
    {"sample-size", 1, nullptr, 'r'},
    {"no-dump-samples", 0, nullptr, 'n'},
    {"allow-deletion", 0, nullptr, 'd'},
    {"help", 2, nullptr, 'h'},
};

struct runner_settings_t {
    enum {
        ALGO_NOT_SET,
        SJOIN_OPT,
        SJOIN,
        SINLJOIN,
    } algorithm = ALGO_NOT_SET;
    std::string query_name; // not checked in parse
    std::string infile; // not checked in parse
    std::string outfile; // not checked in parse
    std::string meter_outfile; // not checked in parse
    enum {
        NO_WINDOW,
        TUMBLING,
        SLIDING,
    } window_type = NO_WINDOW;
    UINT8 window_size = 0;
    enum {
        ST_NOT_SET,
        BERNOULLI,
        SWOR,
        SWR,
    } sampling_type = ST_NOT_SET;
    union {
        UINT8 sample_size;
        double sampling_rate;
    } sample_param;
    bool no_dumping = false;
    bool allow_deletion = false;
    bool measure_update_only = false;
    std::vector<std::string> additional_args; // not checked in parse
} runner_settings;

void print_help(
    const char *progname,
    bool full) {
    std::cerr << "usage: "
        << progname
        << " <options> [<additional_args>]"
        << std::endl;
    std::cerr << "usage: "
        << progname
        << " -h/--help"
        << std::endl;
    std::cerr << "usage: "
        << progname
        << " -h<query_name>/--help=<query_name>"
        << std::endl;
    
    const std::string &query_name = runner_settings.query_name;
    if (!query_name.empty()) {
        if (query_name == "linear_road") {
            print_linear_road_help();
        } else if (query_name.find("tpcds") == 0) {
            print_tpcds_query_help(query_name);
        } else {
            std::cerr << "[error] unknown query name: "
                << query_name
                << std::endl;
        }
    }

#define TAB "    "
#define IND TAB TAB TAB TAB TAB TAB TAB TAB TAB
    if (full) {
        std::cerr << "Options:\n"
            TAB "-q, --query <query name>"
            TAB TAB "Available queries:\n"
            IND TAB "linear_road\n"
            IND TAB "tpcds_qx\n"
            IND TAB "tpcds_qy\n"
            IND TAB "tpcds_qz\n"

            TAB "-a, --algorithm <algorithms>"
            TAB "Available algorithms:\n"
            IND TAB "sjoin_opt (sjoin w/ foreign key opt.)\n"
            IND TAB "sjoin (sjoin w/o foreign key opt.)\n"
            IND TAB "sinljoin (symmetric nested loop)\n"

            TAB "-i, --input <FILE>" TAB TAB TAB "  Input file\n"

            TAB "-o, --output <FILE>" TAB TAB TAB " Output file\n"

            TAB "-m, --meter <FILE>"
            TAB TAB TAB "  Periodic throughput output file\n"
            
            TAB "-w, --window-type <window type>"
            " Available types:\n"
            IND TAB "no_window (default)\n"
            IND TAB "tumbling\n"
            IND TAB "sliding\n"
            IND "Must select tumbling or sliding for\n"
            IND "linear_road; and no_window for tpcds\n"

            TAB "-t, --window-size <window size>"
            " Required if using tumbling/sliding\n"

            TAB "-s, --sampling-type <sampling type>\n"
            IND "Available types:\n"
            IND TAB "bernoulli\n"
            IND TAB "swor (sampling without replacement)\n"
            IND TAB "swr (sampling with replacement)\n"

            TAB "-r, --sample-size <sample size/rate>\n" 
            IND "Floating point sampling rate in (0.0, 1.0)\n"
            IND "for bernoulli; or positive integer sample\n"
            IND "size for swor/swr\n"

            TAB "-n, --no-dump-samples"
            TAB TAB "   Don't dump samples to output file\n"

            TAB "-d, --allow-deletion"
            TAB TAB TAB "Allow deletion in input\n"

            TAB "-h[query_name], --help=[query_name]\n"
            IND "Print the full help and exit.\n"
            IND "If query name is specified, its query-\n"
            IND "specific additional args are also printed.\n"
            IND "Note there is no space between -h and arg\n"
            IND "and there is an = between --help and arg.\n"
            << std::flush;
    }
#undef IND
#undef TAB
}

int parse_cmd_args(int argc, char *argv[]) {
    opterr = 0;
    std::string sample_size_str;
    for (int ret = 0; ret != -1; ) {
        switch (ret = getopt_long(
                    argc, argv, getopt_optstring, getopt_options, 0)) {
        case 'q': 
            runner_settings.query_name = optarg;
            break;
        case 'a':
            if (!strcmp(optarg, "sjoin_opt")) {
                runner_settings.algorithm = runner_settings_t::SJOIN_OPT;
            } else if (!strcmp(optarg, "sjoin")) {
                runner_settings.algorithm = runner_settings_t::SJOIN;
            } else if (!strcmp(optarg, "sinljoin")) {
                runner_settings.algorithm = runner_settings_t::SINLJOIN;
            } else {
                std::cerr << "[ERROR] unknown algorithm: " << optarg
                    << std::endl;
                return 1;
            }
            break;
        case 'i':
            runner_settings.infile = optarg;
            break;
        case 'o':
            runner_settings.outfile = optarg;
            break;
        case 'm':
            runner_settings.meter_outfile = optarg;
            break;
        case 'w':
            if (!strcmp(optarg, "no_window")) {
                runner_settings.window_type = runner_settings_t::NO_WINDOW;
            } else if (!strcmp(optarg, "tumbling")) {
                runner_settings.window_type = runner_settings_t::TUMBLING;
            } else if (!strcmp(optarg, "sliding")) {
                runner_settings.window_type = runner_settings_t::SLIDING;
            } else {
                std::cerr << "[ERROR] unknown window type: " << optarg
                    << std::endl;
                return 1;
            }
            break;
        case 't':
            errno = 0;
            runner_settings.window_size =
                std::strtoull(optarg, nullptr, 0);
            if (errno != 0) {
                std::cerr << "[ERROR] invalid window size: " << optarg
                    << std::endl;
                return 1;
            }
            break;
        case 's':
            if (!strcmp(optarg, "bernoulli")) {
                runner_settings.sampling_type =
                    runner_settings_t::BERNOULLI;
            } else if (!strcmp(optarg, "swor")) {
                runner_settings.sampling_type = runner_settings_t::SWOR;
            } else if (!strcmp(optarg, "swr")) {
                runner_settings.sampling_type = runner_settings_t::SWR;
            } else {
                std::cerr << "[ERROR] invalid sampling type: " << optarg
                    << std::endl;
                return 1;
            }
            break;
        case 'r':
            // Wait until we are out of the loop so that
            // we know if we are expecting an integer (sample size for
            // swor/swr) or a floating point number (sampling rate for
            // bernoulli).
            sample_size_str = optarg;
            break;
        case 'n':
            runner_settings.no_dumping = true; 
            break;
        case 'd':
            runner_settings.allow_deletion = true;
            break;
        case 'h':
            if (optarg) {
                runner_settings.query_name = optarg;
            } else {
                runner_settings.query_name = "";
            }
            print_help(argv[0], true);
            return 2;
        case '?':
            std::cerr << "[ERROR] unknown option: " << argv[optind - 1]
                << std::endl;
            return 1;
        case ':':
            std::cerr << "[ERROR] missing argument for option: "
                << argv[optind - 1]
                << std::endl;
            return 1;
        case 1:
            runner_settings.additional_args.emplace_back(optarg);
            break;
        case -1:
            // done with parsing
            break;
        default:
            RT_ASSERT(false); // unreachable
        }
    }

    if (runner_settings.window_type != runner_settings_t::NO_WINDOW) {
        if (runner_settings.window_size == 0) {
            std::cerr << "[ERROR] a postive wiindow size required "
                "for windowed queries" << std::endl;
            return 1;
        }
    }
    if (runner_settings.algorithm == runner_settings_t::ALGO_NOT_SET) {
        std::cerr << "[ERROR] algorithm not specified"
            << std::endl;
        return 1;
    }
    if (runner_settings.sampling_type == runner_settings_t::ST_NOT_SET) {
        std::cerr << "[ERROR] sampling type not specified"
            << std::endl;
        return 1;
    }
    if (runner_settings.sampling_type == runner_settings_t::BERNOULLI) {
        errno = 0;
        runner_settings.sample_param.sampling_rate =
            std::strtod(sample_size_str.c_str(), nullptr);
        if (errno != 0 ||
            runner_settings.sample_param.sampling_rate <= 0.0 ||
            runner_settings.sample_param.sampling_rate >= 1.0) {
            std::cerr << "[ERROR] invalid sampling rate: "
                << sample_size_str << std::endl;
            return 1;
        }
    } else {
        RT_ASSERT(
            runner_settings.sampling_type == runner_settings_t::SWOR ||
            runner_settings.sampling_type == runner_settings_t::SWR);
        errno = 0;
        runner_settings.sample_param.sample_size =
            std::strtoull(sample_size_str.c_str(), nullptr, 0);
        if (errno != 0 ||
            runner_settings.sample_param.sample_size < 0) {
            std::cerr << "[ERROR] invalid sample size: "
                << sample_size_str << std::endl;
            return 1;
        }

        if (runner_settings.sample_param.sample_size == 0) {
            std::cout << "sampling disabled. measure update only." << std::endl;
            runner_settings.measure_update_only = true;
        }
    }

    return 0;
}

Query *create_query() {
    Query *query = nullptr;
    bool is_opt = false;
    switch (runner_settings.algorithm) {
    case runner_settings_t::SJOIN_OPT:
        is_opt = true;
        intentional_fallthrough;
    case runner_settings_t::SJOIN:
        query = g_root_mmgr->newobj<SJoinQuery>(g_root_mmgr, runner_settings.measure_update_only);
        break;
    case runner_settings_t::SINLJOIN:
        query = g_root_mmgr->newobj<SINLJoinSamplingQuery>(g_root_mmgr);
        break;
    default:
        RT_ASSERT(false, "invalid runner_settings.algorithm %d",
                (int) runner_settings.algorithm);
    }

    switch (runner_settings.window_type) {
    case runner_settings_t::NO_WINDOW:
        query->set_unwindowed(runner_settings.allow_deletion);
        break;
    case runner_settings_t::TUMBLING:
        query->set_tumbling_window(runner_settings.window_size);
        break;
    case runner_settings_t::SLIDING:
        query->set_sliding_window(runner_settings.window_size);
        break;
    }

    switch (runner_settings.sampling_type) {
    case runner_settings_t::BERNOULLI:
        query->set_bernoulli_sampling(
            runner_settings.sample_param.sampling_rate);
        break;
    case runner_settings_t::SWOR:
        query->set_sampling_without_replacement(
            runner_settings.sample_param.sample_size);
        break;
    case runner_settings_t::SWR:
        query->set_sampling_with_replacement(
            runner_settings.sample_param.sample_size);
        break;
    default:
        // unreachable
        RT_ASSERT(false, "unexpected runner_settings.sampling_type");
    }

    if (runner_settings.no_dumping) {
        query->set_no_dumping();
    }
    
    int err_code = 0;
    if (runner_settings.query_name == "linear_road") {
        // TODO
        err_code = setup_linear_road_query(
            query, runner_settings.additional_args);
    } else if (runner_settings.query_name.find("tpcds_") == 0) {
        // TODO
        err_code = setup_tpcds_query(
            query,
            runner_settings.query_name,
            is_opt,
            runner_settings.additional_args);
    } else {
        err_code = 1;
    }
    
    if (err_code != 0) {
        query->~Query();
        g_root_mmgr->deallocate(query);
        return nullptr;
    }
    return query;
}


int main(int argc, char *argv[]) {
    create_root_memmgrs();
    init_typeinfos();

    setup_signal_handlers();
    
    Query *query = nullptr;
    int ret_code = parse_cmd_args(argc, argv);
    if (ret_code == 1) {
        print_help(argv[0], false);
    }
    if (ret_code != 0) {
        goto done;
    }

    query = create_query();
    if (!query) {
        ret_code = 1;
        goto done;
    }

    query->execute(
        runner_settings.infile,
        runner_settings.outfile,
        runner_settings.meter_outfile);
    //TODO update execute_minibatch to make it usable with
    //the current implementation.

done:
    if (query) {
        query->~Query();
        g_root_mmgr->deallocate(query);
    }
    destroy_root_memmgrs();

    return ret_code;
}

////////////////////////////////////////////////
// Old driver impl. for old query interface ////
// Kept for reference only. Don't use.      ////
////////////////////////////////////////////////
//inline void print_available_queries() {
//    std::cout << "available queries:" << std::endl;
//    std::cout << "\tLRoadChainJoinSamplingSJoin" << std::endl;
//    std::cout << "\tLRoadChainJoinSamplingSINLJoin" << std::endl;
//    std::cout << "\tTPCDSQueryJoinSamplingSJoin" << std::endl;
//    std::cout << "\tTPCDSQueryJoinSamplingSINLJoin" << std::endl;
//}

//int run_query(
//    int argc,
//    char *argv[]) {
//    
//    Query *query = nullptr;
//    int ret_code = 0;
//    int qname_idx = 1;
//    std::string query_name;
//    std::string infile;
//    std::string outfile;
//    std::string meter_outfile;
//    UINT8 batch_size = 0;
//    bool no_dumping = false;
//
//    if (argc < 2) {
//        ret_code = 1;
//        goto print_usage;
//    }
//    
//    if (!strcmp(argv[qname_idx], "-m")) {
//        if (argc <= qname_idx + 2) {
//            ret_code = 1;
//            goto print_usage;
//        }
//        meter_outfile = argv[qname_idx + 1];
//        qname_idx = qname_idx + 2;
//    }
//
//    if (!strcmp(argv[qname_idx], "-b")) {
//        if (argc <= qname_idx + 2) {
//            ret_code = 1;
//            goto print_usage;
//        }
//        batch_size = std::strtoull(argv[qname_idx + 1], nullptr, 0);
//        if (batch_size < 0) {
//            ret_code = 1;
//            goto print_usage;
//        }
//        qname_idx = qname_idx + 2;
//    }
//
//    if (!strcmp(argv[qname_idx], "-nd")) {
//    if (argc <= qname_idx + 1) {
//        ret_code = 1;
//        goto print_usage;
//    }
//    no_dumping = true;
//    qname_idx = qname_idx + 1;
//    }
//
//    query_name = argv[qname_idx];
//
//    if (query_name == "LRoadChainJoinSamplingSJoin") {
//        typedef LRoadChainJoinSampling<SJoinQuery> Q;
//        query = g_root_mmgr->newobj<Q>();
//    } else if (query_name == "LRoadChainJoinSamplingSINLJoin") {
//        typedef LRoadChainJoinSampling<SINLJoinSamplingQuery> Q;
//        query = g_root_mmgr->newobj<Q>();
////    } else if (query_name == "EDGARJoinSamplingSJoin") {
////        typedef EDGARJoinSampling<SJoinQuery> Q;
////        query = g_root_mmgr->newobj<Q>();
////    } else if (query_name == "EDGARJoinSamplingSINLJoin") {
////        typedef EDGARJoinSampling<SINLJoinSamplingQuery> Q;
////        query = g_root_mmgr->newobj<Q>();
////    } else if (query_name == "FQueryJoinSamplingSJoin") {
////        typedef FQueryJoinSampling<SJoinQuery> Q;
////        query = g_root_mmgr->newobj<Q>();
////    } else if (query_name == "FQueryJoinSamplingSINLJoin") {
////        typedef FQueryJoinSampling<SINLJoinSamplingQuery> Q;
////        query = g_root_mmgr->newobj<Q>();
////    } else if (query_name == "SQueryJoinSamplingSJoin") {
////        typedef SQueryJoinSampling<SJoinQuery> Q;
////        query = g_root_mmgr->newobj<Q>();
////    } else if (query_name == "SQueryJoinSamplingSINLJoin") {
////        typedef SQueryJoinSampling<SINLJoinSamplingQuery> Q;
////        query = g_root_mmgr->newobj<Q>();
////    } else if (query_name == "TQueryJoinSamplingSJoin") {
////        typedef TQueryJoinSampling<SJoinQuery> Q;
////        query = g_root_mmgr->newobj<Q>();
////    } else if (query_name == "TQueryJoinSamplingSINLJoin") {
////        typedef TQueryJoinSampling<SINLJoinSamplingQuery> Q;
////        query = g_root_mmgr->newobj<Q>();
//    } else if (query_name == "TPCDSQueryJoinSamplingSJoin") {
//        typedef TPCDSQueryJoinSampling<SJoinQuery> Q;
//        query = g_root_mmgr->newobj<Q>();
//    } else if (query_name == "TPCDSQueryJoinSamplingSINLJoin") {
//        typedef TPCDSQueryJoinSampling<SINLJoinSamplingQuery> Q;
//        query = g_root_mmgr->newobj<Q>();
//    } else { 
//        ret_code = 1;
//        goto print_usage;
//    }
//    
//    // only lroad queries require separate function call at this stage
//    // setup of edgar queries is additional_setup() only
//    if (0 == strncmp(query_name.c_str(), "LRoadChainJoin", strlen("LRoadChainJoin"))) {
//        if ((ret_code = setup_lroad_chain_join(query, argc, argv, qname_idx))) {
//            goto fail;
//        }
////    } else if (0 == strncmp(query_name.c_str(), "FQueryJoinSampling", strlen("FQueryJoinSampling"))) {
////    if ((ret_code = setup_f_query_join(query, argc, argv, qname_idx))) {
////        goto fail;
////    }
////    } else if (0 == strncmp(query_name.c_str(), "SQueryJoinSampling", strlen("SQueryJoinSampling"))) {
////    if ((ret_code = setup_s_query_join(query, argc, argv, qname_idx))) {
////        goto fail;
////    }
////    } else if (0 == strncmp(query_name.c_str(), "TQueryJoinSampling", strlen("TQueryJoinSampling"))) {
////    if ((ret_code = setup_t_query_join(query, argc, argv, qname_idx))) {
////        goto fail;
////    }
//    } else if (0 == strncmp(query_name.c_str(), "TPCDSQueryJoinSampling", strlen("TPCDSQueryJoinSampling"))) {
//        if ((ret_code = setup_tpc_ds_query_join(query, argc, argv, qname_idx))) {
//            goto fail;
//        }
//    } else if (0 == strncmp(query_name.c_str(), "EDGARJoinSampling", strlen("EDGARJoinSampling"))) {
//        // nothing
//    } else {
//        ret_code = 1;
//        goto fail;
//    }
//
//    
//    //if ((ret_code = query->additional_setup(argc, argv, qname_idx))) {
//    //    goto fail;
//    //}
//   
//    if (no_dumping) {
//    query->set_no_dumping();
//    }
// 
//    // the check argc >= 4 should have been done by the previous setup
//    infile = argv[qname_idx + 1];
//    outfile = argv[qname_idx + 2];
//
//    if (batch_size == 0) {
//        query->execute(infile, outfile, meter_outfile);
//    } else {
//        query->execute_minibatch(infile, outfile, meter_outfile, batch_size);
//    }
//    
//    goto fail;
//
//print_usage:
//    std::cout << "usage: " << argv[0] << " [-m <meter_outfile>] [-b <batch_size>] [-nd] <query> <infile> <outfile> <args...>"
//        << std::endl;
//    print_available_queries();
//
//fail:
//    if (query) {
//        query->~Query();
//        g_root_mmgr->deallocate(query);
//    }
//
//    return ret_code;
//}
//
