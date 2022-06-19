## About the engine

Walgoritam is a key-value storage engine that features many different advanced programming structures, providing the best user experience while having lightning fast request processing capabilities regardless of the database size.  

### Request options

Walgoritam can process the following list of requests:  
- PUT \<key\> \<value\>  
- GET \<key\>  
- DEL \<key\>  
- GET_TOTAL_KEYS  
- GET_REQ_PER_KEY \<key\>  
- PUT_HLL \<key\> \<p_value\> 
- PUT_CMS \<key\> \<epsilon_value\> \<delta_value\>

### Request descriptions

PUT -> Puts the key-value pair into the engine  
GET -> Retreives the corresponding value from the engine  
DEL -> Removes the specified key-value pair from the engine  
GET_TOTAL_KEYS -> Returns unique key count from the engine  
GET_REQ_PER_KEY -> Returns unique request count from the engine for the specified key  
PUT_HLL -> Creates and puts a custom HyperLogLog structure into the engine with the parameters specified  
PUT_CMS -> Creates and puts a custom CountMinSketch structure into the engine with the parameters specified  

Note: In order to retreive custom HLL or CMS structures from the engine, in the GET request, put \_HLL and \_CMS before the actual key value respectively. Same rules apply to the DEL request as well.
