from grpc_interceptor import ServerInterceptor
import grpc
from app.core.security import security_manager


#TODO: use it with server later to check for JWT tokens
class AuthenticationServerInterceptor(ServerInterceptor):

    def _get_header(self, context: grpc.ServicerContext, header_name: str):
        authorization_header = None
        for key, value in context.invocation_metadata():
            if key == header_name:
                authorization_header = value
                break
        if authorization_header is None:
            raise Exception("Authorization header not found")
        return authorization_header
    

    def _get_token(self, context: grpc.ServicerContext):
        authorization_header = self._get_header(context, 'authorization')
        
        if not authorization_header.startswith('Bearer '):
            raise Exception("Invalid authorization header, must start with Bearer")
        
        return authorization_header.split(' ')[1]

    def intercept(self, method, request, context: grpc.ServicerContext, method_name):
        # Call the RPC. It could be either unary or streaming
        #TODO check if the header is called authorization and has header in it
        try:
            token = self._get_token(context)
            payload = security_manager.verify_token(token)
            context._authenticated_user_id = payload
        except Exception as e:
            context.set_code(grpc.StatusCode.UNAUTHENTICATED)
            context.set_details(str(e))
            raise
        
        try:
            response_or_iterator = method(request, context)
        except Exception as e:
            # If it was unary, then any exception raised would be caught
            # immediately, so handle it here.
            context.set_code(grpc.StatusCode.INTERNAL)
            context.set_details('Internal server error')
            raise
        # Check if it's streaming
        if hasattr(response_or_iterator, "__iter__"):
            # Now we know it's a server streaming RPC, so the actual RPC method
            # hasn't run yet. Delegate to a helper to iterate over it so it runs.
            # The helper needs to re-yield the responses, and we need to return
            # the generator that produces.
            return self._intercept_streaming(response_or_iterator)
        else:
            # For unary cases, we are done, so just return the response.
            return response_or_iterator

    def _intercept_streaming(self, iterator):
        try:
            for resp in iterator:
                yield resp
        except Exception as e:
            raise grpc.RpcError(grpc.StatusCode.INTERNAL, str(e))
        
