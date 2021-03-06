2019-01-22 Big Data 220: Lecture 3

- Discussing homework assignment 2: Enron email
  - OK if solution takes up to 8 hours to process.
  - It is possible to craft a solution that completes in as little as 15 minutes but not expected.
  - Important to learn the concept, don't treat the deadline like a super hard deadline.
- Modern data warehouse
  - Use Enterprise Data warehouse (EDW) for it's strength
    - Can't process all types of modern sources/types of data today
  - Use Big Data for it's strength
  - Combined data lake + data warehouse
    - Grow a data warehouse vs Extended a data warehouse
    - Data lake doesn't replace a data warehouse, often the main reason for data lakes to fail
  - Integrate the two
    - Push v pull
    - Data lake as a data source into a warehouse
  - IKEA model
    - Easily organized once you know what you're looking for.
    - Really difficult if you don't know what you're after.
    - Not suitable for building things from scratch (eg: raw table, only prepared tables)
  - Auto junk yard
    - Some basic means to track where things are
      - Groups of one type car/model
    - Much more generalized approach to organizing
    - Really need something else to make this lot useful
- Data lake
  - Repository for analyzing large quantities of disparate data in native (raw) formats
    - N number of data types
  - Reduce upfront work
    - Easy to get new data in
  - Large volumes
    - Almost unlimited space (thanks cloud storage)
    - Structure & unstructured
  - Traditional data warehousing
    - ![traditionaldatawarehousing](/images/2020/01/traditionaldatawarehousing.png)
  - Modernizing an existing DW
    - ![modernizingexistingdw](/images/2020/01/modernizingexistingdw.png)
  - New virtualized options
    - eg: Redshift Spectrum, data warehouse solution that enables querying data directly in storage (S3).
  - Co-exists
    - DW and Big Data platforms complement each other
    - Usually extending an existing DW
      - Alternative is to add MPP database, still best suited for structured data
    - Adds value to DW investments
  - How a data lake fits into a data strategy
    - DL as a staging area for DW
      - ![dlstaging](/images/2020/01/dlstaging.png)
    - DL as a destinate for archival data
    - Ingest new types of data
    - DL Zones
      - Transient/Temporary space
        - holding space to organize raw data
      - Raw data / staging
        - Immutable, retain history
      - Curated
        - cleaned and organized, ready to deliver to other systems
      - Analytics Sandbox
        - provide space for exploratory activity (data science & machine learning)
  - Processing
    - Ingest/store in native format
    - Analyze in place to determine value
      - aka schema-on-read
    - Adding value to date
      - integrate with data warehouse
      - schema on write (we should have rules now)
    - Data virtualization
      - Not forcing user to choose from querying from DW or DL, let the solution figure it out, SQL'ize your DL
  - Organizing
    - Optimal structure based on data retrieval
    - Should be largely self-documenting
    - Based on
      - Subject area (not as useful is knowledge is siloed)
      - Security boundaries (based on access)
      - Downstream purpose (based on usage)
      - etc
      - eg:
        - ![rawdatastagingpartitions](/images/2020/01/rawdatastagingpartitions.png)
        - ![rawstagingpartitionsexample](/images/2020/01/rawstagingpartitionsexample.png)
- Major categories of data
    - Batch, most analytics are still orientated this way
    - Streaming
    - Can usually process batch using streaming (very quick)
      - eg: having an hourly process
    - Strive for simple infrastructure
    - Batch data
      - Logs
        - File based
        - Best candidate for streaming
      - Very unstructured text
        - Document based data
      - RDBMS
        - Operational/Transactional (OLTP)
        - Enterprise Data Warehouse
    - Batch, integration
      - Capture files and store in DL
      - Goal is to acquire raw data to allow immediate analysis
      - Avoid combining, transforming, filtering at this later
        - Do this as part of curation
      - Maintain immutability
    - Common open-source tools (non-streaming)
      - Syslog/Rsyslog (log processing)
      - Flume (Hadoop-y log shipper)
      - FluentD (unified logging layer)
      - Elastic (Logstash/Beats)
      - Graylog (centralized log management)
    - Various "ETL" vendor tools
      - eg: Talend, Informatica, etc
  - Data staging
    - HDFS
      - Needs infrastructure, still best overall cost/performance fo on-prem solutions
    - S3 (or other cloud)
      - Performance trade off
      - Huge performance boost if utilizing cloud managed services
      - Watch cost, especially for data movement OUT of cloud
  - Data formats
    - Can cause cryptic errors in application struggles
      - FileNotFoundException
      - MissingOutputLocation
    - Text file
      - Simple, worst performance
      - Try to use a physical delimited if any change a human needs to read it
      - Avoid if data has structure
        - Not JSON, XML - should be remodeled as a more performant format
    - HDFS Sequence Files
      - Avoids small file problem
      - Only applicable to HDFS
    - Parquet
      - Column major format, striped
        - Column optimized based on queries
        - Spark has additional optimization for Parquet
          - Automative partition discovery
        - Very good/fast compression
        - Spark + Parquet > Hive
    - ORC
      - Similar to Parquet specific to Hive
      - Only recommended for Hive-heavy shop
- Do we still need operational DB and EDW?
  - Yes
  - To handle
    - Transactions, joins, querying
  - Capable to handle 'big data' workloads via MPP
    - Vertical vs. horizontal scale
    - Cost can become prohibitive
      - Storage, compute, licensing
  - Pull data out and stage it for analysis
    - OLTP -> Data Lake
      - advanced analytics outside the bound for an relational DB
    - EDW -> Data Lake
      - Data size prohibitive
      - Offload complex queries
      - Combined hot/cold data into single query
