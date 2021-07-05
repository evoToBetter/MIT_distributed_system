# impl problems or key point

## election restriction(paper 5.4.1)
When leader election, need to make sure all the committed entries from previous terms are present on each new leader from the moment of its election.  
So we need to make sure candidate who win the election has the entries which is up-to-date. The up-to-date means comparing the index and term of last entries. The logs has later term is more up-to-date. If logs has same terms, the logs has longer entries is more up-to-date.  
## append request timeout
To make the request failed fast, need to limit request timeout shorter than the heartBeatInterval.  
## heart beat timeout
To make leader heart beat finish in one interval, every request wait time need to be smaller than heart beat interval, so cluster will election new leader faster.

## final test result
Test (2C): basic persistence ...  
  ... Passed --   4.2  3   81   28490    6  
Test (2C): more persistence ...  
  ... Passed --  31.3  5 1158  203101   16  
Test (2C): partitioned leader and one follower crash, leader restarts ...  
  ... Passed --   3.6  3   47   13024    4  
Test (2C): Figure 8 ...  
  ... Passed --  32.2  5  946  254164   34  
Test (2C): unreliable agreement ...  
  ... Passed --   2.6  5 1426  546437  246  
Test (2C): Figure 8 (unreliable) ...  
  ... Passed --  34.7  5 1892  496123   18  
Test (2C): churn ...  
  ... Passed --  16.4  5 6974 14851402 1168  
Test (2C): unreliable churn ...  
  ... Passed --  16.7  5 1250  562719  103  
PASS  