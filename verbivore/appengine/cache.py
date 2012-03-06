from google.appengine.api import memcache
import logging
import tasks as tasks

class CacheEntity( object ):

	def __init__( self ):
		self.frequency = 0

	def increment_frequency( self ):
		self.frequency = self.frequency + 1

class CacheWord( CacheEntity ):
	
	def __init__( self, word ):
		self.word = word
		super( CacheWord, self ).__init__()

	def commit( self ):
		tasks.commit_word( self.word, self.frequency )

class CacheLink( CacheEntity ):
	
	def __init__( self, cacheword_root, cacheword_following ):
		self.cacheword_root = cacheword_root
		self.cacheword_following = cacheword_following
		super( CacheLink, self ).__init__()

	def commit( self ):
		tasks.commit_forward_link( self.cacheword_root.word, self.cacheword_following.word, self.frequency )

def get_words():
	word_cache = memcache.get( "word_cache" )
	if word_cache is None:
		word_cache = {}
	return word_cache

def set_words( cache ):
	memcache.set( "word_cache", cache, 0 )

def get_word( word, increment_frequency=False ):

	word = word.lower()

	cached_word = None
	writeback = False
	word_cache = get_words()	
	
	if( word in word_cache ):
		cached_word = word_cache[ word ]
	else:
		cached_word = CacheWord( word )
		word_cache[ word ] = cached_word
		writeback = True

	if increment_frequency:
		cached_word.increment_frequency()
		writeback = True

	if writeback:
		set_words( word_cache )

	return cached_word

def set_word( word, entity ):
	word_cache = get_words()
	word_cache[ word ] = entity
	set_words( word_cache )

def get_links():
	link_cache = memcache.get( "link_cache" )
	if link_cache is None:
		link_cache = {}
	return link_cache

def set_links( link_cache ):
	memcache.set( "link_cache", link_cache, 0 )

def get_links_for_word( root_word ):
	link_cache = get_links()
	link = None
	if( root_word in link_cache ):
		forward_links = link_cache[ root_word ]	
	else:
		forward_links = {}
	return forward_links

def set_links_for_word( root_word, links ):
	link_cache = get_links()
	link_cache[ root_word ] = links
	set_links( link_cache )

def get_link_for_cachewords( cacheword_root, cacheword_next ):
	
	forward_links = get_links_for_word( cacheword_root.word )
	to_word = cacheword_next.word

	if( to_word in forward_links ):
		link = forward_links[ to_word ]
	else:	
		link = CacheLink( cacheword_root, cacheword_next )
	return link

def set_link_for_words( root_word, to_word, link ):
	forward_links = get_links_for_word( root_word )
	forward_links[ to_word ] = link
	set_links_for_word( root_word, forward_links )

def add_forward_link( root_word, to_word ):

	if root_word is not None:
		cacheword_root = get_word( root_word, True )
	if to_word is not None:
		cacheword_next = get_word( to_word, False )

	if cacheword_root is not None and cacheword_next is not None:
		link = get_link_for_cachewords( cacheword_root, cacheword_next )
		link.frequency = link.frequency + 1
		# write back out to cache
		set_link_for_words( root_word, to_word, link )

def commit():
		
		word_cache = get_words()
		
		logging.debug( "word_cache has %d entries" % len( word_cache ) )

		for word in word_cache:
			cached_word = word_cache[ word ]
			logging.debug( "%s has a frequency of %d" % ( word, cached_word.frequency ) )
			cached_word.commit()

		link_cache = get_links()

		logging.debug( "link_cache has %d entries" % len( link_cache ) )

		for root_word in link_cache:
			forward_links = link_cache[ root_word ]
			for following_word in forward_links:
				cached_link = forward_links[ following_word ]
				cached_link.commit()
