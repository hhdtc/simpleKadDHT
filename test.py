# Python 3 program to find length of  
# the longest increasing subsequence 
# whose adjacent element differ by 1 
import sys 
  
# function that returns the length  
# of the longest increasing subsequence 
# whose adjacent element differ by 1 
def longestSubsequence(a, n): 
      
    # stores the index of elements 
    mp = {i:0 for i in range(13)} 
  
    # stores the length of the longest 
    # subsequence that ends with a[i] 
    dp = [0 for i in range(n)] 
  
    maximum = -sys.maxsize - 1
  
    # iterate for all element 
    index = -1
    for i in range(n): 
          
        # if a[i]-1 is present before 
        # i-th index 
        if ((a[i] - 1 ) in mp): 
              
            # last index of a[i]-1 
            lastIndex = mp[a[i] - 1] - 1
  
            # relation 
            dp[i] = 1 + dp[lastIndex] 
        else: 
            dp[i] = 1
  
        # stores the index as 1-index as we  
        # need to check for occurrence, hence  
        # 0-th index will not be possible to check 
        mp[a[i]] = i + 1
  
        # stores the longest length 
        if (maximum < dp[i]): 
            maximum = dp[i] 
            index = i 
  
    # We know last element of sequence is 
    # a[index]. We also know that length 
    # of subsequence is "maximum". So We 
    # print these many consecutive elements 
    # starting from "a[index] - maximum + 1" 
    # to a[index]. 
    for curr in range(a[index] - maximum + 1,  
                      a[index] + 1, 1): 
        print(curr, end = " ") 
  
# Driver Code 
if __name__ == '__main__': 
    a = [3, 10, 3, 11, 4, 5,  
                6, 7, 8, 12] 
    n = len(a) 
    longestSubsequence(a, n) 
  
# This code is contributed by 
# Surendra_Gangwar 