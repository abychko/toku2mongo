// The part of or5.js that ends in doing something with geo indexes
t = db.jstests_or5geo;
t.drop();

t.ensureIndex( {a:1} );
t.ensureIndex( {b:1} );

assert.eq.automsg( "'BasicCursor'", "t.find( {$or:[{a:2},{b:3},{}]} ).explain().cursor" );
assert.eq.automsg( "'BasicCursor'", "t.find( {$or:[{a:2},{b:3},{c:4}]} ).explain().cursor" );
printjson( t.find( {$or:[{a:2},{b:3}]} ).sort( {c:1} ).explain() );
assert.eq.automsg( "'BasicCursor'", "t.find( {$or:[{a:2},{b:3}]} ).sort( {c:1} ).explain().cursor" );
e = t.find( {$or:[{a:2},{b:3}]} ).sort( {a:1} ).explain();
assert.eq.automsg( "'IndexCursor a_1'", "e.cursor" );
assert.eq.automsg( "1", "e.indexBounds.a[ 0 ][ 0 ].$minElement" );
assert.eq.automsg( "1", "e.indexBounds.a[ 0 ][ 1 ].$maxElement" );

t.ensureIndex( {c:1} );

t.save( {a:2} );
t.save( {b:3} );
t.save( {c:4} );
t.save( {a:2,b:3} );
t.save( {a:2,c:4} );
t.save( {b:3,c:4} );
t.save( {a:2,b:3,c:4} );

assert.eq.automsg( "7", "t.count( {$or:[{a:2},{b:3},{c:4}]} )" );
assert.eq.automsg( "6", "t.count( {$or:[{a:6},{b:3},{c:4}]} )" );
assert.eq.automsg( "6", "t.count( {$or:[{a:2},{b:6},{c:4}]} )" );
assert.eq.automsg( "6", "t.count( {$or:[{a:2},{b:3},{c:6}]} )" );

assert.eq.automsg( "7", "t.find( {$or:[{a:2},{b:3},{c:4}]} ).toArray().length" );
assert.eq.automsg( "6", "t.find( {$or:[{a:6},{b:3},{c:4}]} ).toArray().length" );
assert.eq.automsg( "6", "t.find( {$or:[{a:2},{b:6},{c:4}]} ).toArray().length" );
assert.eq.automsg( "6", "t.find( {$or:[{a:2},{b:3},{c:6}]} ).toArray().length" );

for( i = 2; i <= 7; ++i ) {
assert.eq.automsg( "7", "t.find( {$or:[{a:2},{b:3},{c:4}]} ).batchSize( i ).toArray().length" );
assert.eq.automsg( "6", "t.find( {$or:[{a:6},{b:3},{c:4}]} ).batchSize( i ).toArray().length" );
assert.eq.automsg( "6", "t.find( {$or:[{a:2},{b:6},{c:4}]} ).batchSize( i ).toArray().length" );
assert.eq.automsg( "6", "t.find( {$or:[{a:2},{b:3},{c:6}]} ).batchSize( i ).toArray().length" );
}

t.ensureIndex( {z:"2d"} );

assert.eq.automsg( "'GeoSearchCursor'", "t.find( {z:{$near:[50,50]},a:2} ).explain().cursor" );
assert.eq.automsg( "'GeoSearchCursor'", "t.find( {z:{$near:[50,50]},$or:[{a:2}]} ).explain().cursor" );
assert.eq.automsg( "'GeoSearchCursor'", "t.find( {$or:[{a:2}],z:{$near:[50,50]}} ).explain().cursor" );
assert.eq.automsg( "'GeoSearchCursor'", "t.find( {$or:[{a:2},{b:3}],z:{$near:[50,50]}} ).explain().cursor" );
assert.throws.automsg( function() { return t.find( {$or:[{z:{$near:[50,50]}},{a:2}]} ).toArray(); } );

t.drop();
