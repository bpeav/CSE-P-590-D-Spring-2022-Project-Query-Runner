# IMDB JOB Queries Project
Code for my final project in CSE P 590 D Spring 2022 - Special Topics: Advanced Topics in Data Management.

The ```ARM``` directory has an ARM template to deploy a single instance of an Azure SQL (SQL Server), Azure PostgreSQL, Azure MySQL, and MariaDB database all on the same Gen 5, 2 vcore, and 50 GB storage SKU. Note - this is a production-level Azure SKU and as of May 2022, it costs approximately $15/day total to have all 4 databases provisioned.

The purpose of this code is to run the JOB benchmark queries from the paper [How Good Are Query Optimizers, Really?](https://www.vldb.org/pvldb/vol9/p204-leis.pdf) on all 4 databases.

Loading the appropriate IMDB data into the databases was accomplished using the imdbpy2sql script per the instructions provided in that paper. The FTP site referenced in that paper doesn't appear to be up any longer, but I was able to find a mirror here - https://ftp.sunet.se/mirror/archive/ftp.sunet.se/pub/tv+movies/imdb/

Before running the console app in this repo, you'll need to provide a configuration JSON file via the [Secret Manager in Visual Studio](https://docs.microsoft.com/en-us/aspnet/core/security/app-secrets?view=aspnetcore-6.0&tabs=windows#secret-manager) (if you're familiar with .NET Core, the code should be fairly straightforward to modify with a different configuraiton source if you prefer), with JSON of this form:
```json
"DatabaseConnectionStrings": {
    "SqlServer": "<connection string here>",
    "PostgreSql": "<connection string here>",
    "MySql": "<connection string here>",
    "MariaDb": "<connection string here>"
}
```