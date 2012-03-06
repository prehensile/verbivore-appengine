import cache
import datastore as datastore
from google.appengine.api import memcache
from google.appengine.api import taskqueue
import logging
import datetime
from scanner import Scanner
import re

ADD_FORWARD_LINK		= "add_forward_link"
FINALISE_LINKS			= "finalise_links"
COMMIT_FORWARD_LINK		= "commit_forward_link"
COMMIT_WORD				= "commit_word"
CONSUME_TEXT 			= "consume_text"

# quotas
QUEUE_DAILY_LIMIT		= 100000

def get_queue_length():
	queue_length = memcache.get( "queue_length" )
	if queue_length is None:
		queue_length = 0
	return queue_length

def set_queue_length( length ):
	memcache.set( "queue_length", length, 0 )

def add_task( url, params=None ):
	queue_length = get_queue_length()
	eta = datetime.datetime.now()
	if queue_length >= QUEUE_DAILY_LIMIT:
		num_days = int( queue_length / QUEUE_DAILY_LIMIT )
		eta += datetime.timedelta( num_days )
		params.eta = eta
		logging.debug( "queue length is %d (%d days)" % ( queue_length, num_days ) )
	taskqueue.add( url=url, params=params, eta=eta )
	queue_length = queue_length + 1
	set_queue_length( queue_length )


def handle_task( task_name, params ):
	
	ql = get_queue_length()
	ql -= 1
	set_queue_length( ql )

	if task_name == ADD_FORWARD_LINK:
		add_forward_link_worker( params )

        tokens_processed = memcache.get( "tokens_processed" )
        if( tokens_processed is None ):
            memcache.add( "tokens_processed", 0, 0 )
            tokens_processed = 0
        tokens_processed = tokens_processed + 1
        memcache.set( "tokens_processed", tokens_processed )

	if task_name == FINALISE_LINKS:
		finalise_links_worker()

	if task_name == COMMIT_WORD:
		commit_word_worker( params )

	if task_name == COMMIT_FORWARD_LINK:
		commit_forward_link_worker( params )

	if task_name == CONSUME_TEXT:
		consume_text_worker( params )

ROOT_WORD_KEY = "root_word"
NEXT_WORD_KEY = "next_word"

def add_forward_link( root_word, next_word ):
	params = { ROOT_WORD_KEY : root_word, NEXT_WORD_KEY : next_word }
	add_task( url='/tasks/add_forward_link', params = params )

def add_forward_link_worker( params ):
	root_word = None
	next_word = None
	if ROOT_WORD_KEY in params:
		root_word = params[ ROOT_WORD_KEY ]
	if NEXT_WORD_KEY in params:
		next_word = params[ NEXT_WORD_KEY ]
	cache.add_forward_link( root_word, next_word )

def finalise_links():
	add_task( url='/tasks/finalise_links' )	

def finalise_links_worker():
	cache.commit()

FREQUENCY_KEY	= 'frequency'

def commit_forward_link( root_word, to_word, frequency ):
	params = { ROOT_WORD_KEY : root_word, NEXT_WORD_KEY : to_word, FREQUENCY_KEY : frequency }
	add_task( url='/tasks/commit_forward_link', params=params )

def commit_forward_link_worker( params ):
	root_word = params[ ROOT_WORD_KEY ]
	next_word = params[ NEXT_WORD_KEY ]
	frequency = params[ FREQUENCY_KEY ]
	datastore.commit_forward_link( root_word, next_word, frequency )

WORD_KEY = 'word'

def commit_word( word, frequency ):
	params = { WORD_KEY : word, FREQUENCY_KEY : frequency }
	add_task( url='/tasks/commit_word', params=params )

def commit_word_worker( params ):
	word = params[ WORD_KEY ]
	frequency = params[ 'frequency' ]
	datastore.commit_word( word, frequency )

OFFSET_KEY = "offset"
TASK_TIME = datetime.timedelta( minutes=9 )
#TASK_TIME = datetime.timedelta( seconds=9 )

def consume_text( text ):
	datastore.write_current_blob( text )
	params = { OFFSET_KEY : 0 }
	add_task( url='/tasks/consume_text', params=params )

def consume_text_worker( params ):

	then 			= datetime.datetime.now()

	offset			= 0
	if OFFSET_KEY in params:
		offset = int( params[ OFFSET_KEY ] )

	text 			= datastore.read_current_blob( offset )
	scanner 		= Scanner( text )
	current_word 	= None
	num_tasks 		= 0 
	pattern			= re.compile( r"[\w']+|[.,!?;]" )
	timeout			= False
	elapsed			= None

	while not timeout and not scanner.eos():
		
		token = scanner.scan( pattern )
		if current_word is not None:
			add_forward_link( current_word, token )
			num_tasks = num_tasks + 1
		current_word = token
		scanner.get()

		elapsed = datetime.datetime.now() - then
		if elapsed >= TASK_TIME:
			offset = scanner.pos 
			add_task( "/tasks/consume_text", { OFFSET_KEY : offset } )
			logging.debug( "consume_text_worker hit timelimit, will resume at offset %d" % offset )
			timeout = True
	
	logging.debug( "scanned %d tokens in %d seconds" % (num_tasks,elapsed.total_seconds() ) )

	if scanner.eos():
		finalise_links()


	
	