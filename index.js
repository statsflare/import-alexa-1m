var request = require('request');
var unzip   = require('unzip');
var csv2    = require('csv2');
var AWS = require('aws-sdk');
var sqs = new AWS.SQS();
var async = require('async')

exports.handler = function (event, context) {
	var queue = []
	var $domains_per_sqs = 300
	var $i = 0;

	function csv_item(r, idx ) {
		idx = parseInt(r[0] / $domains_per_sqs)
		if (!queue[idx] ) queue[idx] = []
		queue[idx].push(r)
	}

	async.parallel([
		// download top 1m and save to queue
		function(cb) {
			request.get('http://s3.amazonaws.com/alexa-static/top-1m.csv.zip')
				.pipe(unzip.Parse())
				.on('entry', function (entry) {
					entry.pipe(csv2())
					.on('data', csv_item)
					.on('finish', function() {
						console.log("finished csv parse")
						finished_csv = true
						cb()
					})
				}).on('finish', function() {
					console.log("fnished piping to unzip")
				})
		},
	], function() {
		console.log("reached end")
		async.each(queue, function(item,cb) {
			var params = {
			  MessageBody: JSON.stringify(item),
			  QueueUrl: process.env['sqs-domain-add'],
			};
			sqs.sendMessage(params, function(err, data) {
				//if (err) console.log(err, err.stack); // an error occurred
				//else     console.log(data);
				cb()
			});
		}, function() {
			console.log("finished, number of slices=", queue.length )
			context.done()
		})

	})
}
