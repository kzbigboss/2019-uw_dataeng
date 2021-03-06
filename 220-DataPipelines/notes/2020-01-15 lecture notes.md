2019-01-15 Big Data 220: Lecture 2

- Agenda
  - Review of week one assignment
  - Talking about building big data apps
  - "Case study"
  - Talk about week two assignment
- Week one assignment
  - Someone asked about loading a CSV but only a select few columns
    - Not possible with this format as CSV is a tabular format.  You need to read through the rows to understand each of the columns that the rows contains.  Only after processing all the rows can you drop column.
    - This is the benefit of using a format like parquet - a columnar file format that avoids reading an entire dataset when you're only interested in a subset.
  - Basically loaded the file, changed the casing on the interested column, grouped by the interested column, then aggregated the final result.
- Building pipelines
  - More than just code/processing
    - Cluster planning
    - Tuning
    - DevOps
    - Data management
    - SecOps
    - !(data science)
  - ![buildingdatapiplinesflowchart](/images/2020/01/buildingdatapiplinesflowchart.png)
- Acquiring data
  - Often not an easy task unless your source is a database.
  - Notebooks serve as a good point to explore.
- Discovery/cleaning
  - Dealing with missing/incomplete
    - Delete rows, delete columns, add default value, add imputed value
  - Attempt to understand why data is missing or incomplete
    - Bad parsing, hidden characters, random, user error
  - Normalization, after ingestion - keep intake clean
  - Basic transforms
    - Formats, standards, casing, whitespace, hidden characters, nulls/""/0
- Integrating / Linking
  - Two main approaches
    - Declarative
      - Manually DECLARE all the rules, usually based on historical knowledge
      - eg: if (fname == fname) and (lname == lname) and (dob == dob) then match
    - Probabilistic linking
      - "fuzzy match"
      - score/confidence based
      - fueled by machine learning
- Case studies
  - ![2014casestudy](/images/2020/01/2014casestudy.png)
- Assignment
  - Data discovery
