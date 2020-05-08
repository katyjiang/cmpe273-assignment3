import sys
import socket
import pickle
import functools

from sample_data import USERS
from server_config import NODES
from pickle_hash import serialize_GET, serialize_PUT, serialize_DELETE, hash_code_hex
from node_ring import NodeRing
from bloom_filter import BloomFilter

BUFFER_SIZE = 1024
ring = NodeRing(nodes=NODES)
    
def lru_cache(max_size):
    cache = {}
    cache_keys = []

    def lrucache_dec(fn):
        def cached_fn(*args):
        	if args in cache:
        		del cache_keys[cache_keys.index(args)]
        		cache_keys.append(args)
        		return cache[args]
        			
        	retval = fn(*args)
        	cache[args] = retval
        	cache_keys.append(args)
        		
        	if len(cache_keys) > max_size:
        		del cache[cache_keys[0]]
        		del cache_keys[0]
        	return retval
        return cached_fn
    return lrucache_dec
    	
class UDPClient():
    def __init__(self, host, port):
        self.host = host
        self.port = int(port)
        self.hash_codes = set() # Will store str object
        self.bloom_filter = BloomFilter(100000, 0.05)

    # Argument 'user' is bytes object apply pickle.dumps(..)
    # on a python dictionary, refer to
    # sample_data.py for the format of the user python dictionary.
    # The reason we use bytes object is that functools.lru_cache doesn't
    # accept dictionary
    # Return response as str object

        
        
        
    @lru_cache(5)
    def put(self, user_pickle):    
        data_bytes, key = serialize_PUT(pickle.loads(user_pickle))
        
        self.bloom_filter.add(key) # Add to bloom filter, be careful must add key (or say hash code)
        
        response = self.__send(data_bytes)
        self.hash_codes.add(response.decode())
        return response.decode()
    
    # Argument 'hc' is a str object
    # Return bytes object or None
    @lru_cache(5)
    def get(self, hc):
        if self.bloom_filter.is_member(hc):
            data_bytes, key = serialize_GET(hc)
            return self.__send(data_bytes)
        else:
            return None
        

    # Argument 'hc' is a str object
    # Return str object, should be "{'success'}" or "{'fail'}"
    @lru_cache(5)
    def delete(self, hc):
        if self.bloom_filter.is_member(hc):
            data_bytes, key = serialize_DELETE(hc)
            response = self.__send(data_bytes)
            self.hash_codes.remove(hc) # Remember to remove from hash_codes
            return response.decode()
        else:
            return "{'fail'}"
        

    def get_hash_codes(self):
        # Return a copy of the set to avoid user's change affect the internal set
        return self.hash_codes.copy()
    
    # Prefix with double underscore is python style to
    # define private function
    def __send(self, request):
        print('Connecting to server at {}:{}'.format(self.host, self.port))
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            s.sendto(request, (self.host, self.port))
            response, ip = s.recvfrom(BUFFER_SIZE)
            return response
        except socket.error:
            print("Error! {}".format(socket.error))
            exit()
    
        

if __name__ == "__main__":
    clients = [
        UDPClient(server['host'], server['port'])
        for server in NODES
    ]
    
    # PUT all users.
    for u in USERS:
        hc = hash_code_hex(pickle.dumps(u))
        node = ring.get_node(hc)
        print(clients[NODES.index(node)].put(pickle.dumps(u)))

    total_cached_clients = 0
    for client in clients:
        total_cache_clients = total_cached_clients + len(client.get_hash_codes())
    print(f"Number of Users={len(USERS)}\nNumber of Users Cached={total_cached_clients}")
    
    # GET all users.
    for client in clients:
        for hc in client.get_hash_codes():
            print(hc)
            print(client.get(hc))
        

    # DELETE all users.
    for client in clients:
        for hc in client.get_hash_codes():
            print(hc)
            print(client.delete(hc))
