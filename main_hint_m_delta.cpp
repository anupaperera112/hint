/******************************************************************************
 * Project:  hint
 * Purpose:  Indexing interval data - Dynamic HINT^m with delta indexes
 * Author:   Extended from original HINT by Bouros, Christodoulou, Mamoulis
 ******************************************************************************
 * CLI driver for HINT_M_Dynamic: supports initial data load, operations file
 * (inserts/deletes), and standard query file processing.
 ******************************************************************************/

#include "getopt.h"
#include "def_global.h"
#include "./containers/relation.h"
#include "./indices/hint_m_delta.h"



void usage()
{
    cerr << endl;
    cerr << "PROJECT" << endl;
    cerr << "       HINT^m Dynamic: HINT^m with Delta Insert/Delete Indexes" << endl << endl;
    cerr << "USAGE" << endl;
    cerr << "       ./query_hint_m_delta.exec [OPTION]... [DATA] [QUERIES]" << endl << endl;
    cerr << "DESCRIPTION" << endl;
    cerr << "       -? or -h" << endl;
    cerr << "              display this help message and exit" << endl;
    cerr << "       -v" << endl;
    cerr << "              activate verbose mode" << endl;
    cerr << "       -q predicate" << endl;
    cerr << "              set predicate type: \"GOVERLAPS\"" << endl;
    cerr << "       -m bits" << endl;
    cerr << "              set the number of bits; if not set, auto-determined via cost model" << endl;
    cerr << "       -t" << endl;
    cerr << "              use top-down traversal instead of bottom-up" << endl;
    cerr << "       -u operations_file" << endl;
    cerr << "              set the operations file for inserts/deletes" << endl;
    cerr << "              format: each line is \"I start end\" (insert) or \"D recordId\" (delete)" << endl;
    cerr << "       -i insert_threshold" << endl;
    cerr << "              set the insert delta merge threshold; by default 1000" << endl;
    cerr << "       -d delete_threshold" << endl;
    cerr << "              set the delete delta merge threshold; by default 1000" << endl;
    cerr << "       -r runs" << endl;
    cerr << "              set the number of runs per query; by default 1" << endl << endl;
    cerr << "EXAMPLES" << endl;
    cerr << "       ./query_hint_m_delta.exec -m 10 -q gOVERLAPS samples/AARHUS-BOOKS_2013.dat samples/AARHUS-BOOKS_2013_20k.qry" << endl;
    cerr << "       ./query_hint_m_delta.exec -m 10 -q gOVERLAPS -u ops.txt -i 500 -d 200 -v samples/AARHUS-BOOKS_2013.dat samples/AARHUS-BOOKS_2013_20k.qry" << endl << endl;
}


int main(int argc, char **argv)
{
    Timer tim;
    Relation R;
    HINT_M_Dynamic *idxR;
    size_t totalResult = 0, queryresult = 0, numQueries = 0;
    double totalIndexTime = 0, totalQueryTime = 0, querytime = 0, avgQueryTime = 0;
    double totalOpsTime = 0;
    Timestamp qstart, qend;
    RunSettings settings;
    char c;
    double vmDQ = 0, rssDQ = 0, vmI = 0, rssI = 0;
    string strPredicate = "", strOptimizations = "";
    const char *opsFile = nullptr;
    unsigned int insertThreshold = 1000;
    unsigned int deleteThreshold = 1000;
    size_t numInserts = 0, numDeletes = 0;


    // Parse command line input
    settings.init();
    settings.method = "hint_m_delta";
    while ((c = getopt(argc, argv, "?hvq:m:tu:i:d:r:")) != -1)
    {
        switch (c)
        {
            case '?':
            case 'h':
                usage();
                return 0;

            case 'v':
                settings.verbose = true;
                break;

            case 'q':
                strPredicate = toUpperCase((char*)optarg);
                break;

            case 'm':
                settings.numBits = atoi(optarg);
                break;

            case 't':
                settings.topDown = true;
                break;

            case 'u':
                opsFile = optarg;
                break;

            case 'i':
                insertThreshold = atoi(optarg);
                break;

            case 'd':
                deleteThreshold = atoi(optarg);
                break;

            case 'r':
                settings.numRuns = atoi(optarg);
                break;

            default:
                cerr << endl << "Error - unknown option '" << c << "'" << endl << endl;
                usage();
                return 1;
        }
    }


    // Sanity check
    if (argc-optind != 2)
    {
        usage();
        return 1;
    }
    if (!checkPredicate(strPredicate, settings))
    {
        if (strPredicate == "")
            cerr << endl << "Error - predicate type not defined" << endl << endl;
        else
            cerr << endl << "Error - unknown predicate type \"" << strPredicate << "\"" << endl << endl;
        usage();
        return 1;
    }
    settings.dataFile = argv[optind];
    settings.queryFile = argv[optind+1];


    // Load data
    R.load(settings.dataFile);
    settings.maxBits = int(log2(R.gend-R.gstart)+1);

    // If no bits specified, auto-determine
    if (settings.numBits == 0)
        settings.numBits = determineOptimalNumBitsForHINT_M(R, 0.1);

    process_mem_usage(vmDQ, rssDQ);


    // Build dynamic HINT^m index
    tim.start();
    idxR = new HINT_M_Dynamic(R, settings.numBits, settings.maxBits,
                              insertThreshold, deleteThreshold);
    totalIndexTime = tim.stop();
    process_mem_usage(vmI, rssI);


    // Process operations file (inserts / deletes) if provided
    if (opsFile != nullptr)
    {
        ifstream fOps(opsFile);
        if (!fOps)
        {
            cerr << endl << "Error - cannot open operations file \"" << opsFile << "\"" << endl << endl;
            return 1;
        }

        string line;
        tim.start();
        while (getline(fOps, line))
        {
            if (line.empty())
                continue;

            if (line[0] == 'I' || line[0] == 'i')
            {
                Timestamp s, e;
                if (sscanf(line.c_str() + 1, "%d %d", &s, &e) == 2)
                {
                    idxR->insert(s, e);
                    numInserts++;
                }
                else
                {
                    cerr << "Warning - malformed insert line: " << line << endl;
                }
            }
            else if (line[0] == 'D' || line[0] == 'd')
            {
                RecordId rid;
                if (sscanf(line.c_str() + 1, "%d", &rid) == 1)
                {
                    idxR->remove(rid);
                    numDeletes++;
                }
                else
                {
                    cerr << "Warning - malformed delete line: " << line << endl;
                }
            }
            else
            {
                cerr << "Warning - unknown operation: " << line << endl;
            }
        }
        totalOpsTime = tim.stop();
        fOps.close();

        if (settings.verbose)
        {
            cout << endl;
            cout << "Operations processed" << endl;
            cout << "  Inserts : " << numInserts << endl;
            cout << "  Deletes : " << numDeletes << endl;
            printf( "  Time    : %f secs\n", totalOpsTime);
            cout << endl;
        }
    }


    // Execute queries
    ifstream fQ(settings.queryFile);
    if (!fQ)
    {
        cerr << endl << "Error - cannot open query file \"" << settings.queryFile << "\"" << endl << endl;
        return 1;
    }

    size_t sumQ = 0;
    if (settings.verbose)
        cout << "Query\tPredicate\tMethod\tBits\tStrategy\tResult\tTime" << endl;
    while (fQ >> qstart >> qend)
    {
        sumQ += qend-qstart;
        numQueries++;

        double sumT = 0;
        for (auto r = 0; r < settings.numRuns; r++)
        {
            switch (settings.typePredicate)
            {
                case PREDICATE_GOVERLAPS:
                    if (settings.topDown)
                    {
                        tim.start();
                        queryresult = idxR->executeTopDown_gOverlaps(RangeQuery(numQueries, qstart, qend));
                        querytime = tim.stop();
                    }
                    else
                    {
                        tim.start();
                        queryresult = idxR->executeBottomUp_gOverlaps(RangeQuery(numQueries, qstart, qend));
                        querytime = tim.stop();
                    }
                    break;

                default:
                    cerr << "Error - predicate not supported by HINT_M_Dynamic (only gOVERLAPS)" << endl;
                    return 1;
            }
            sumT += querytime;
            totalQueryTime += querytime;

            if (settings.verbose)
                cout << "[" << qstart << "," << qend << "]\t" << strPredicate << "\t" << settings.method << "\t" << idxR->getNumBits() << "\t" << ((settings.topDown)? "top-down": "bottom-up") << "\t" << queryresult << "\t" << querytime << endl;
        }
        totalResult += queryresult;
        avgQueryTime += sumT/settings.numRuns;
    }
    fQ.close();


    // Report
    idxR->getStats();
    cout << endl;
    cout << "HINT^m Dynamic (Delta Index)" << endl;
    cout << "============================" << endl;
    cout << "Input" << endl;
    cout << "  Num of intervals          : " << R.size() << endl;
    cout << "  Domain size               : " << (R.gend-R.gstart) << endl;
    cout << "  Avg interval extent [%]   : "; printf("%f\n", R.avgRecordExtent*100/(R.gend-R.gstart));
    cout << endl;
    cout << "Index" << endl;
    cout << "  Num of bits               : " << idxR->getNumBits() << endl;
    cout << "  Num of partitions         : " << idxR->numPartitions << endl;
    cout << "  Num of Originals          : " << idxR->numOriginals << endl;
    cout << "  Num of replicas           : " << idxR->numReplicas << endl;
    cout << "  Num of empty partitions   : " << idxR->numEmptyPartitions << endl;
    printf( "  Avg partition size        : %f\n", idxR->avgPartitionSize);
    printf( "  Read VM [Bytes]           : %ld\n", (size_t)(vmI-vmDQ)*1024);
    printf( "  Read RSS [Bytes]          : %ld\n", (size_t)(rssI-rssDQ)*1024);
    printf( "  Indexing time [secs]      : %f\n", totalIndexTime);
    cout << endl;
    cout << "Delta Indexes" << endl;
    cout << "  Insert threshold          : " << insertThreshold << endl;
    cout << "  Delete threshold          : " << deleteThreshold << endl;
    cout << "  Pending inserts           : " << idxR->numDeltaInserts << endl;
    cout << "  Pending deletes           : " << idxR->numDeltaDeletes << endl;
    cout << "  Total merges performed    : " << idxR->numMerges << endl;
    if (opsFile != nullptr)
    {
        cout << "  Total inserts processed   : " << numInserts << endl;
        cout << "  Total deletes processed   : " << numDeletes << endl;
        printf( "  Operations time [secs]    : %f\n", totalOpsTime);
    }
    cout << endl;
    cout << "Queries" << endl;
    cout << "  Predicate type            : " << strPredicate << endl;
    cout << "  Strategy                  : " << ((settings.topDown) ? "top-down": "bottom-up") << endl;
    cout << "  Num of runs per query     : " << settings.numRuns << endl;
    cout << "  Num of queries            : " << numQueries << endl;
    cout << "  Avg query extent [%]      : "; printf("%f\n", (((float)sumQ/numQueries)*100)/(R.gend-R.gstart));
    cout << "  Total result [";
#ifdef WORKLOAD_COUNT
    cout << "COUNT]      : ";
#else
    cout << "XOR]        : ";
#endif
    cout << totalResult << endl;
    printf( "  Total querying time [secs]: %f\n", totalQueryTime/settings.numRuns);
    printf( "  Avg querying time [secs]  : %f\n\n", avgQueryTime/numQueries);
    printf( "  Throughput [queries/sec]  : %f\n\n", numQueries/(totalQueryTime/settings.numRuns));

    delete idxR;


    return 0;
}
