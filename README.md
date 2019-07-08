# T-Test-in-Pyspark
Conduct t-test between two sets of biosets (i.e. A &amp; B) with the help of spark transformations, dataframes and user defined functions.

<br>
<b>Input : </b>
Each group (A and B) will have a set of biosets. A bioset represents a patient data and each record has the following format:
<gene_id><,><reference><,><gene_value>
where
  <gene_id> is a string
  <reference> is a string
  <gene_value> is a floating point number such as 1.2, 2.0, ...

<br><b>How to run:</b>
<ol><li> First store the text files of biosets (groupA.txt and groupB.txt) and ttest.py in one folder.
  <li>Open commnad prompt and navigate to that folder (like cd 'E:\New_folder' )
    <li>Call program (ttest.py) in the command line:
      <br>python ttest.py biosets_path groupA.txt groupB.txt
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
    
    
