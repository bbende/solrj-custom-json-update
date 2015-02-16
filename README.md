solrj-custom-json-update
=================

This repository contains example code that shows how to index custom JSON in Apache 
Solr using SolrJ. 

This blog post by LucidWords shows how the JSON update handler works using curl:
[Indexing Custom Json Data](http://lucidworks.com/blog/indexing-custom-json-data/).

IndexJSONTest contains two tests:

* testAddCustomJsonWithContentStreamUpdateRequest - demonstrates how to use the built-in SolrJ
classes to perform the update
* testAddCustomJsonWithJSONUpdateRequest - demonstrates how to use a custom update request to 
perform the update