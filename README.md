# T-Test-in-Pyspark (SPARK APPLICATION)

One of the biggest advantage of using big data technologies is that it gives output way more faster than the typical sequential execution. The amount of data we are generating per day is tremendous i.e. 2.5 quintillion bytes data per day (Refer: https://www.domo.com/learn/data-never-sleeps-5?aid=ogsm072517_1&sf100871281=1). In order to analyze such huge data, one has to use big data technologies.
  
The Objective of this project is simple i.e. to make use of big data technologies and conduct t-test on large amount of data. The program will compute and show us the results in matter of few seconds rather than days (if done without the use of big data technologies like SPARK).

Create an Spark application which will conduct t-test between two sets of biosets with the help of spark transformations, dataframes and user defined functions.

<b><br> Refer Application Code here: https://github.com/kpratikin/T-Test-in-Pyspark/blob/master/ttest.py</b>


<b><br>The details of inputs, outputs and how to run the application are described below: </b>
<br>
<b>Input : </b>
Each group (A and B) will have a set of biosets. A bioset represents a patient data and each record has the following format:
(gene_id)(,)(reference)(,)(gene_value)
where
  <gene_id> is a string
  <reference> is a string
  <gene_value> is a floating point number such as 1.2, 2.0, ...
<br> Example:<br>
  g1,r1,1.4 <br>
  g2,r2,3.6 <br>
  g3,r1,1.0 <br>

<br><b>How to run:</b>
<ol><li> First store the text files of biosets (groupA.txt and groupB.txt), ttest.py and biosets in one folder.
  <li>Open commnad prompt and navigate to that folder (example cd 'E:\New_folder' )
    <li>Call program (ttest.py) in the command line:
      <br>spark-submit ttest.py biosets_path groupA.txt groupB.txt
where 
    * biosets_path points to a directory where all biosets 
       (for both groups)are defined.
    * groupA.txt represents a set of biosets
    * groupB.txt represents a set of biosets 

For example: biosets_path can be "E:\New_folder\biosets"
$ ls -l 'E:/New_folder/biosets'
11001
11002
11009
71001
71002
71009
21000
21001
21008
21009
21333
21334
21335
21336
...
$ notepad groupA.txt
11001
11002
11009
21334
21335
21336
$ notepad groupB.txt
11001
71001
71002
71009
21000
21001
21008
</ol>    
    
<br><b>Output:</b> The output is a pair:<br>
  (gene_id, (your-p-value, python-p-value, mean-A, mean-B))
<br>Where:<br>
mean-A = mean of values for groupA <br>
mean-B = mean of values for groupB <br>
your-p-value = p-value (score) of TTest for groupA and groupB (my calculation) <br>
python-p-value = p-value (score) of TTest for groupA and groupB (ttest_ind calculation) <br>
<br>
<p align="center"><img src="https://github.com/kpratikin/T-Test-in-Pyspark/blob/master/Output.PNG">
 <br>Figure: T-test using Spark (ttest.py output)
 </p>
<br>
Note: <ul><li>for a given gene_id, if there are no values in groupA, then output should look like:<br>
  (gene_id, (0.0, 0.0, 0.0, mean-B))
<li>for a given gene_id, if there are no values in groupB, then output should look like:<br>
  (gene_id, (0.0, 0.0, mean-A, 0.0)).  
