import json
import feedparser
from datetime import datetime
from typing import Dict, List, Any, Optional

RSS_UNIFIED_SCHEMA = {
  "_id": "ulid",
	"schema_version": "1.0.0",
	"status": "meta|extracted|processed|labeled|embedded",
	
	"run": {
		"run_id": "ulid",
		"exec_ts": "2025-08-27T06:10:00Z",
	},
	
	"source": {
		"feed_id": "hash_of_feed_url",
		"feed_url": "https://example.com/feed.xml",
		"feed_name": "Example Blog",
	},
	
	"partition": { "year": "2025", "month": "08", "day": "27" },
	
	"audit": {
		"created_at": "2025-08-27T06:10:00Z",
		"extracted_at": "2025-08-27T06:10:00Z",
		"processed_at": "2025-08-27T06:10:00Z",
		"labeled_at": "2025-08-27T06:10:00Z",
		"embedded_at": "2025-08-27T06:10:00Z",
	},

	"rss": {
		"feed": {
			"title": "String",
			"title_detail": {
				"type": "String",
				"language": "String|null",
				"base": "String",
				"value": "String"
			},
			"links": [
				{ "href": "String", "rel": "String", "type": "String" }
				],
				"link": "String",
				"subtitle": "String",
				"subtitle_detail": {
					"type": "String",
					"language": "String|null",
					"base": "String",
					"value": "String"
				},
				"updated": "String",
				"updated_parsed": "Array",
				"language": "String|null",
				"sy_updateperiod": "String",
				"sy_updatefrequency": "String",
				
				"image": {
					"href": "String",
					"title": "String",
					"title_detail": {
						"type": "String",
						"language": "String|null",
						"base": "String",
						"value": "String"
					},
					"links": [
						{ "rel": "String", "type": "String", "href": "String" }
					],
					"link": "String"
				},
				"generator_detail": { "name": "String" },
				"generator": "String",
				
				"publisher": "String",
				"publisher_detail": { "email": "String" },
			
      	"docs": "String - RSS spec URL",
      	"ttl": "String|Number",
      	"authors": [ { "email": "String" } ],
      	"author": "String",
      	"author_detail": { "email": "String" },
      	"day": "String",
      	"skipdays": "String"
			},

			"entries": [
				{
					"title": "String",
					"title_detail": {
						"type": "String",
						"language": "String|null",
						"base": "String",
						"value": "String"
					},
					"links": [
						{	"rel": "String", "type": "String", "href": "String"	}
					],
					"link": "String",

					"authors": [ {"name": "String"} ],
					"author": "String",
					"author_detail": { "name": "String" },
					
					"published": "String (RFC2822)",
					"published_parsed": "Array[int]",
					
					"updated": "String (ISO8601|RFC2822)",
					"updated_parsed": "Array[int]",
					
					"tags": [
						{	"term": "String", "scheme": "String|null", "label": "String|null" }
					],
					
					"id": "String",
					"guidislink": "Boolean",
					
					"summary": "String",
					"summary_detail": {
						"type": "String",
						"language": "String|null",
						"base": "String",
						"value": "String"
					},

					"content": [ { "type": "String", "language": "String|null", "base": "String",	"value": "String" } ],
					
					"footnotes": [ { "id": "String", "text": "String" } ],

					"media_content": [
						{ "url": "String", "type": "String", "width": "Number", "height": "Number", "medium": "String" }
					],
					"media_thumbnail": [
						{	"url": "String", "width": "Number", "height": "Number" }
					],

					"arxiv_announce_type": "String",
					"rights": "String",
					"rights_detail": {
						"type": "String",
						"language": "String|null",
						"base": "String",
						"value": "String"
					}
				}
			],

			"source": { "url": "String", "title": "String" },

			"generator": "String",
			"generator_detail": {	"name": "String", "version": "String", "uri": "String" },

			"docs": "String",
			"ttl": "Number",

			"image": { "title": "String", "url": "String", "link": "String", "width": "Number", "height": "Number" },
			
			"copyright": "String",
			"rights": "String",
			"rights_detail": {
				"type": "String",
				"language": "String",
				"base": "String",
				"value": "String"
			}
		}
	}
  