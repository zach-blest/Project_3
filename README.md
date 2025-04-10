
The following table lists the graph files alongside their corresponding MIS files and indicates whether the latter represents a maximal independent set (MIS).

| Graph File          | MIS File               | Is an MIS? |
|---------------------|------------------------|------------|
| small_edges.csv     | small_edges_MIS.csv    | Yes        |
| small_edges.csv     | small_edges_non_MIS.csv| No         |
| line_100_edges.csv  | line_100_MIS_test_1.csv| Yes        |
| line_100_edges.csv  | line_100_MIS_test_2.csv| No         |
| twitter_10000_edges.csv | twitter_10000_MIS_test_1.csv | No  |
| twitter_10000_edges.csv | twitter_10000_MIS_test_2.csv | Yes |

The following table shows the performance metrics for various graph files based on the number of iterations and runtime.

| Graph File            | Iterations | Runtime  |
|-----------------------|------------|----------|
| small_edges.csv       | 2          | 1s       |
| line_100_edges.csv    | 3          | 1s       |
| twitter_100_edges.csv | 2          | 1s       |
| twitter_1000_edges.csv| 2          | 1s       |
| twitter_10000_edges.csv| 3         | 2s       |

The following table lists different core configurations and their performance metrics, including running time, iterations, remaining active vertices, and verification of the maximal independent set (MIS).

| Configuration | Running Time [s] | Iterations | Remaining Active Vertices     | verifyMIS |
|---------------|------------------|------------|-------------------------------|-----------|
| 3x4 cores     | 201              | 4          | 6943795, 31969, 351, 0    | ✅       |
| 4x2 cores     | 532             | 5          | 6593276, 37452, 401, 5, 0     | ✅       |
| 2x2 cores     | 710             | 5          | 6453457, 36830, 371, 2, 0     | ✅       |