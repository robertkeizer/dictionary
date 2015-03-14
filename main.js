var config		= require( "config" );
var wikipedia		= require( "wtf_wikipedia" );
var elasticsearch	= require( "elasticsearch" );
var async		= require( "async" );

function queryWikipedia( word, cb ){
	wikipedia.from_api( word, "en", function( markup ){

		if( !wikipedia.parse( markup ).text ){
			return cb( null, { } );
		}

		var _words = { };
		wikipedia.plaintext( markup ).split( " " ).forEach( function( word ){

			if( !word.match(/^[a-zA-Z]*$/) ){
				return;
			}

			if( !_words[word] ){
				_words[word] = 1;
				return;
			}
			_words[word]++;
		} );

		return cb( null, _words );
	} );
};

function queryWord( word, cb ){

	console.log( "queryWord; " + word );

	var _index = config.elasticsearch.index.prefix + word.substr( 0, 1 ).toLowerCase();

	async.waterfall( [ function( cb ){
		client.indices.exists( { index: _index }, function( err, response ){
			if( err ){ return cb( err ); }

			return cb( null, response );
		} );
	}, function( indexExists, cb ){
		if( indexExists ){
			return cb( null );
		}

		client.indices.create( { index: _index }, function( err, response ){
			if( err ){ return cb( err ); }
			return cb( null );
		} );

	}, function( cb ){

		client.get( {
			index: _index, 
			type: 'word',
			id: word
		}, function( err, response ){

			if( response.found ){
				return cb( null, response._source.words );
			}

			queryWikipedia( word, function( err, results ){

				if( err ){ return cb( err ); }

				var _obj = { timestamp: new Date( ), words: results }

				client.create( {
					index: _index,
					type: 'word',
					id: word,
					body: _obj
				}, function( err, response ){
					console.log( "Added '" + word + "' with " + Object.keys( results ).length + " words branching" );
					if( err ){ return cb( err ); }
					if( !response.created ){ return cb( response ); }

					console.log( "Added '" + word + "' with " + Object.keys( results ).length + " words branching" );

					return cb( null, results );
				} );
			} );

		} );
	} ], function( err, results ){
		if( err ){ return cb( err ); }

		if( !results ){
			console.log( "NO RESULTS" );
			return;
		}
		return cb( null, results );
	} );
};

function pushIntoQueue( what ){
	what.forEach( function( word ){
		if( QUEUE.indexOf( word ) < 0 ){
			QUEUE.push( word );
		}
	} );
};

function runQueue( ){
	async.forever( function( cb ){

		async.eachLimit( QUEUE, 10, function( word, cb ){
			queryWord( word, function( err, results ){
				if( err ){ return cb( err ); }
				if( results ){ pushIntoQueue( Object.keys(results) ); }
				return cb( null );
			} );
		}, function( err ){
			if( err ){ return cb( err ); }

			setTimeout( function( ){
				return cb( null );
			}, 5*1000 );
		} );

	}, function( err ){
		console.log( "FATAL ERROR: " + err );
	} );
};

var client	= new elasticsearch.Client( { host: config.elasticsearch.host } );
var QUEUE	= [ ];

queryWord( "computer", function( err, result ){

	if( err ){
		console.log( err );
		return;
	}
	
	pushIntoQueue( Object.keys(result) );

	runQueue( );
} );
