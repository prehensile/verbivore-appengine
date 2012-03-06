from google.appengine.ext import db
from google.appengine.ext import blobstore
from google.appengine.api import files

class VBState( db.Model ):
	current_blob		= blobstore.BlobReferenceProperty()
	blob_position		= db.IntegerProperty()

class VBWord( db.Model ):
	word				= db.StringProperty( required=True )
	frequency			= db.IntegerProperty( required=True, default=0 )

class VBWordForwardLink( db.Model ):
	root_word			= db.ReferenceProperty( VBWord, required=True, collection_name="root_reference_set" )
	next_word		 	= db.ReferenceProperty( VBWord, required=True, collection_name="following_word_set" )
	frequency			= db.IntegerProperty( required=True, default=0 )

def get_word( word ):
	query = VBWord.all()
	query.filter( 'word = ', word )
	entity = query.get()
	if( entity is None ):
		entity = VBWord( word=word )
	return entity

def get_link( entity_root, entity_following ):
	query = VBWordForwardLink.all()
	query.filter( 'root_word = ', entity_root )
	query.filter( 'following_word = ', entity_following )
	entity = query.get()
	if( entity is None ):
		entity = VBWordForwardLink( root_word=entity_root, next_word=entity_following )
	return entity

def commit_word( word, frequency ):
	db_word = get_word( word )
	db_word.frequency = int( frequency )
	db_word.put()

def commit_forward_link( root_word, following_word, frequency ):
	entity_root = get_word( root_word )
	entity_following = get_word( following_word )
	db_link = get_link( entity_root, entity_following )
	db_link.frequency = int( frequency )
	db_link.put()

def get_current_state():
	query = VBState.all()
	state = query.get()
	if( state is None ):
		state = VBState()
	return state

def write_current_blob( text ):

	# write blob
	fn = files.blobstore.create( mime_type='text/plain' )
	with files.open( fn, 'a' ) as f:
		f.write( text )
	files.finalize( fn )

	state = get_current_state()
	state.current_blob = files.blobstore.get_blob_key( fn )
	state.put()

def read_current_blob( offset ):
	# see http://code.google.com/appengine/docs/python/blobstore/blobreaderclass.html
	state = get_current_state()
	blob_key = state.current_blob
	blob_reader = blobstore.BlobReader( blob_key, position=offset)
	return blob_reader.read()
	
