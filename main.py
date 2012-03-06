import webapp2
from google.appengine.api import memcache
from google.appengine.api import namespace_manager
import verbivore.appengine.tasks as tasks 
from verbivore.VerbivoreAppEngine import VerbivoreAppEngine

def namespace_for_path( path ):
    path_components = path.split('/')
    if( len( path_components ) > 1 ):
        return path_components[ 2 ]
    return None

class MainHandler(webapp2.RequestHandler):
    
    def get(self):
        self.response.out.write('<html><body>')
        self.response.out.write("""<form action="/upload" enctype="multipart/form-data" method="post">""")
        self.response.out.write("""Upload File: <input type="file" name="file"><br/>""")
        self.response.out.write("""Namespace: <input type="text" name="tf_namespace"><br/>""")
        self.response.out.write("""<input type="submit" name="submit" value="Submit">""")
        self.response.out.write('</form></body></html>')

        
class UploadHandler(webapp2.RequestHandler):

    def post(self):
    	
        self.response.out.write('<html>')
        
        text_in = self.request.get( "file" )
        namespace_in = self.request.get( "tf_namespace" )

        #self.response.out.write("""<head><meta http-equiv="Refresh" content="3; url=/status/%s" /></head>""" % namespace_in )
        self.response.out.write('<body>')

        if( text_in ):
            vb = VerbivoreAppEngine( namespace_in )
            vb.swallow( text_in )
            self.response.out.write( "Started consuming text. n0m n0m n0m." )
        else:
            self.response.out.write( "No text!" )

        self.response.out.write('</body></html>')
        

class StatusHandler( webapp2.RequestHandler ):
    
    def get( self ):

        namespace = namespace_for_path( self.request.path )
        if( namespace ):
            namespace_manager.set_namespace( namespace )
    
        tokens_processed    = memcache.get( "tokens_processed" )
        num_tokens          = memcache.get( "num_tokens" )

        if( tokens_processed is None ):
            tokens_processed = 0
        if( num_tokens is None ):
            num_tokens = 0

        done = tokens_processed >= num_tokens -1

        self.response.out.write('<html>')
        if( not done ):
             self.response.out.write("""<head><meta http-equiv="Refresh" content="1; url=%s" /></head>""" % self.request.url )
        self.response.out.write('<body>')
        self.response.out.write( "Processed %d of %d tokens in namespace %s" % ( tokens_processed, num_tokens, namespace ) )
        if( done ):
                self.response.out.write('<br/>Done!')
        self.response.out.write('</body></html>')


class TaskHandler(webapp2.RequestHandler):

    def get( self ):
        self.post()

    def post( self ):
        
        path_components = self.request.path.split('/')
        task            = path_components[2]
        
        args = self.request.arguments()
        params = {}
        for arg in args:
            params[ arg ] = self.request.get( arg )
        tasks.handle_task( task, params )

class GenerationHandler( webapp2.RequestHandler ):
    
    def get( self ):

        namespace = namespace_for_path( self.request.path )
        if( namespace ):
            namespace_manager.set_namespace( namespace )

        vb_connector = VerbivoreMachineAppEngine.VerbivoreMachineAppEngine( namespace )
        verbivore = Verbivore.Verbivore( vb_connector )

        str_out = verbivore.spit( 140, "I" )

        self.response.out.write('<html><body>')
        self.response.out.write( str_out )
        self.response.out.write('</form></body></html>')

class MainPage( webapp2.RequestHandler ):
  def get( self ):
      self.response.headers['Content-Type'] = 'text/plain'
      self.response.out.write('Hello, webapp World!')

app = webapp2.WSGIApplication( [	('/', MainHandler),
									('/upload', UploadHandler),
                                    ('/status/.*', StatusHandler),
                                    ('/generate/.*', GenerationHandler),
                                    ('/tasks/.*', TaskHandler) ] )