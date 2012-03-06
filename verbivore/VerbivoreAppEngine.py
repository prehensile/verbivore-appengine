
from __future__ import with_statement
from google.appengine.api import namespace_manager
from random import choice
import logging
from scanner import Scanner
import re

import appengine.cache as cache
from appengine.datastore import VBState
import appengine.tasks as tasks

class VerbivoreAppEngine:
		
	def __init__( self, namespace ):
		logging.debug( "VerbivoreMachineAppEngine.init")
		self.current_cacheword = None
		if( namespace is not None ):
			namespace_manager.set_namespace( namespace )
	
	def set_current_word( self, word, increment_frequency=False ):
		self.current_cacheword = self.get_cached_word( word, increment_frequency )
	
	def current_word( self ):
		if( self.current_cacheword is not None ):
			return self.current_cacheword.word
		return None

	

#	def add_forward_link( self, to_word ):
#		
#		if( self.current_cacheword is not None and to_word is not None and len(to_word) > 0 ):
#			root_word = self.current_cacheword.word
#			link = self.cached_link_for_words(  root_word, to_word )
#			link.frequency = link.frequency + 1
#			# write back out to cache
#			self.set_cached_link_for_words( link, root_word, to_word )

	

	def step_forward( self ):
		
		logging.debug( "VerbivoreMachineAppEngine.step_forward")
		logging.debug( "-> current_entity= %s" % self.current_entity )
		
		if( self.current_entity ):
		
			query = VBWordForwardLink.all()
			query.filter( 'root_word = ', self.current_entity )
		
			candidates = query.fetch( 10 )
		
			logging.debug( "--> found %d candidates" % len( candidates ) )
		
			if( candidates is None or len( candidates ) < 1 ):
				self.current_entity = None
			else:
				forward_link = choice( candidates )
				self.current_entity = forward_link.next_word

	def swallow( self, text ):
		tasks.consume_text( text )

	def resume_current_blob( self ):
		v = 1

	def spit( self, length, start_word=None ):
		
		logging.debug( "Verbivore.spit")

		if( start_word is None ):
			start_word = "."
		self.vb_connector.set_current_word( start_word )
		
		out_string		= ""
		out_length		= len( out_string )
		
		while( len( out_string ) < length ):
			
			self.vb_connector.step_forward()
			word = self.vb_connector.current_word()

			logging.debug( "word: %s" % word )

			if( word is None ):
				break;

			# add to out_string
			if( out_string == "" ):
				out_string = word.capitalize()
			elif( re.match( "\W", word ) ):
				out_string = "%s%s" % (out_string, word)
			else: 
				out_string = "%s %s" % (out_string, word)
			
			out_length = len( out_string )

			if( out_string[-1:] == "." ):
				break
			
		return( out_string )

	