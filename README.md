Couchbase MS SQL Importer
===

This tool allows you to copy the content of a your tables into Couchbase.

The import

* Import all rows as JSON document. All table columns are JSON attributes.
* Optionnaly it is possible to create views
-----

1. Configure an import.properties file with all the parameters

		## SQL Information ##
		sql.connection=jdbc:mysql://192.168.99.19:3306/world?zeroDateTimeBehavior=convertToNull
		sql.username=root
		sql.password=password

		## Couchbase Information ##
		cb.uris=http://localhost:8091/pools
		cb.bucket=default
		cb.password=

		## Import information
		import.tables=ALL
		import.createViews=true
		import.typefield=type
		import.fieldcase=lower

This Project was customized from couchbase sql importer by me.
You want to see more infomation, check these urls: [git](https://www.google.com.tw/url?sa=t&rct=j&q=&esrc=s&source=web&cd=1&cad=rja&uact=8&ved=0ahUKEwijyJTf6JLTAhWGupQKHY97AMUQFggjMAA&url=https%3A%2F%2Fgithub.com%2Ftgrall%2Fcouchbase-sql-importer&usg=AFQjCNGSIrbFo7jF8Qf3iPF75_1ouFY-GQ&sig2=Tp4ezv-OmG6S-BadHhirVg)
[youtube](https://www.google.com.tw/url?sa=t&rct=j&q=&esrc=s&source=web&cd=2&cad=rja&uact=8&ved=0ahUKEwijyJTf6JLTAhWGupQKHY97AMUQtwIIKjAB&url=https%3A%2F%2Fwww.youtube.com%2Fwatch%3Fv%3DxzqBjhYKCLY&usg=AFQjCNGiCSTOPiLLSLx92-hTo4xXdRKe-w&sig2=9xwSza6LLeFf5hLXtnZ8QA)
