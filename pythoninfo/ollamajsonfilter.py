# Ollama JSON-Only Output Filter
# Enforces strict JSON grammar on Ollama model outputs

import json
import re
import requests
import time
from typing import Optional, Dict, Any, Callable

class OllamaJSONFilter:
    def __init__(self, base_url: str = "http://localhost:11434"):
        self.base_url = base_url
        # JSON-valid characters: structural chars, digits, booleans, null, whitespace, string chars
        self.json_token_pattern = re.compile(r'^[{}\[\]:,"\s0-9eE\+\-tfnrul\\\/]+$')
        self.in_string = False
        self.escape_next = False
        self.inthink=True
        
    def is_valid_json_token(self, token: str) -> bool:
        """Check if a token contains only JSON-valid characters"""
        print(token)
        if "</think>"==token:
            self.inthink=False
            return 1

        if (self.inthink):
            return 1

        if not token:
            return 0
            
        # Track string context to allow any characters within strings
        for char in token:
            if self.escape_next:
                self.escape_next = False
                continue
                
            if char == '\\' and self.in_string:
                self.escape_next = True
                continue
                
            if char == '"' and not self.escape_next:
                self.in_string = not self.in_string
                continue
                
            # If we're inside a string, allow any character
            if self.in_string:
                continue
                
            # Outside strings, only allow JSON structural characters
            if not re.match(r'[{}\[\]:.,\s0-9eE\+\-tfnrul]', char):
                return 2
                
        return 0
    
    def reset_string_state(self):
        """Reset string tracking state for new generation"""
        self.in_string = False
        self.escape_next = False
    
    def generate_json_only(self, 
                          model: str, 
                          prompt: str, 
                          schema: Optional[Dict] = None,
                          max_retries: int = 3,
                          temperature: float = 0.1) -> Dict[str, Any]:
        """
        Generate JSON-only output from Ollama with strict token filtering
        
        Args:
            model: Ollama model name
            prompt: Input prompt
            schema: Optional JSON schema for validation
            max_retries: Number of retry attempts on invalid JSON
            temperature: Model temperature (lower = more deterministic)
        """
        
        system_prompt = """You are a JSON generator. Respond ONLY with valid JSON. 
Do not include any explanatory text, comments, or additional content. 
Your entire response must be parseable as JSON.  }
"""
        
        if schema:
            system_prompt += f"\n\nYour JSON must match this schema:\n{json.dumps(schema, indent=2)}"
        
        full_prompt = f"System: {system_prompt}\n\nUser: {prompt}\n\nJSON Response:"
        response = self._generate_with_filter(model, full_prompt, temperature)
        # Validate JSON
        print(response)

        parsed = json.loads(response[0])
        # Validate against schema if provided
        print(parsed)
        if schema:

            try:
                self._validate_schema(parsed, schema)
                return {
                    "success": True,
                    "data": parsed,
                    "raw_response": response[1],
                }
            
            except Exception as e:
                return {
                    "success": False,
                    "data": parsed,
                    "raw_response": response,
                    "json_error": str(e)
                }

        
    def _generate_with_filter(self, model: str, prompt: str, temperature: float) -> str:
        """Generate text with token-level filtering"""
        self.reset_string_state()
        
        payload = {
            "model": model,
            "prompt": prompt,
            "stream": True,
            "options": {
                "temperature": temperature,
                "num_predict": 2048
            }
        }
        
        response = requests.post(
            f"{self.base_url}/api/generate",
            json=payload,
            stream=True
        )
        
        if response.status_code != 200:
            raise Exception(f"Ollama API error: {response.status_code}")
        
        filtered_output = ""
        fulloutput=""
        
        for line in response.iter_lines():
            if line:
                try:
                    chunk = json.loads(line.decode('utf-8'))
                    if 'response' in chunk:
                        token = chunk['response']
                        
                        # Apply token filter
                        fulloutput+= token
                        check=self.is_valid_json_token(token)
                        if check==0:
                            filtered_output += token
                        elif (check==1):
                            pass
                        else:
                            # Skip invalid tokens
                            print(f"Filtered invalid token: '{token}'")
                            
                    if chunk.get('done', False):
                        break
                        
                except json.JSONDecodeError:
                    continue
        
        return [filtered_output.strip(), fulloutput]
    
    def _validate_schema(self, data: Any, schema: Dict) -> None:
        """Basic schema validation (implement more comprehensive validation as needed)"""
        if schema.get('type') == 'object':
            if not isinstance(data, dict):
                raise ValueError("Expected object type")
            
            required = schema.get('required', [])
            for field in required:
                if field not in data:
                    raise ValueError(f"Missing required field: {field}")
            
            properties = schema.get('properties', {})
            for key, value in data.items():
                if key in properties:
                    prop_type = properties[key].get('type')
                    if prop_type == 'string' and not isinstance(value, str):
                        raise ValueError(f"Field {key} must be string")
                    elif prop_type == 'integer' and not isinstance(value, int):
                        raise ValueError(f"Field {key} must be integer")
                    elif prop_type == 'boolean' and not isinstance(value, bool):
                        raise ValueError(f"Field {key} must be boolean")





def checkoutput(result):

    def find_highest_values(dictionary):
        """
        Find the highest value(s) from Invalid, Quoted, and Referred keys,
        apply the transformation logic, and return them in alphabetical order.
        """
        # Apply the transformation logic
        dictionary['Invalid'] = dictionary['Invalid'] * 100 if dictionary['Invalid'] < 1 else dictionary['Invalid']
        dictionary['Quoted'] = dictionary['Quoted'] * 100 if dictionary['Quoted'] < 1 else dictionary['Quoted']
        dictionary['Referred'] = dictionary['Referred'] * 100 if dictionary['Referred'] < 1 else dictionary['Referred']
        
        # Get the three values we're comparing
        values_to_compare = {
            'Invalid': dictionary['Invalid'],
            'Quoted': dictionary['Quoted'],
            'Referred': dictionary['Referred']
        }
        
        # Find the maximum value
        max_value = max(values_to_compare.values())
        
        # Find all keys that have the maximum value
        highest_keys = [key for key, value in values_to_compare.items() if value == max_value]
        
        # Sort alphabetically and join with commas
        highest_keys.sort()
        
        return ','.join(highest_keys)




    dictionary = {
        "Invalid": 0,
        "Quoted": 0,
        "Referred": 0,
        "raw_response":result['raw_response'],
        "valid":False
    }


    try:
        dictionary['Invalid']=float(result["data"]["Invalid"]["type"])
        dictionary['Quoted']=float(result["data"]["Quoted"]["type"])
        dictionary['Referred']=float(result["data"]["Referred"]["type"])

        dictionary['valid']=True
    except:
        try:
            dictionary['Invalid']=float(result["data"]["Invalid"]["value"])
            dictionary['Quoted']=float(result["data"]["Quoted"]["value"])
            dictionary['Referred']=float(result["data"]["Referred"]["value"])
            dictionary['valid']=True
        except Exception as e:
            try:
                dictionary['Invalid']=float(result["data"]["Invalid"])
                dictionary['Quoted']=float(result["data"]["Quoted"])
                dictionary['Referred']=float(result["data"]["Referred"])
                dictionary['valid']=True
            except Exception as e:
                try:
                    dictionary['Invalid']=float(result["data"]["Invalid"]["probability"])
                    Quoted=float(result["data"]["Quoted"]["probability"])
                    dictionary['Referred']['Quoted']=float(result["data"]["Referred"]["probability"])
                    dictionary['valid']=True
                except:
                    pass
    dictionary['Invalid'] = dictionary['Invalid'] * 100 if dictionary['Invalid'] < 1.1 else dictionary['Invalid']
    dictionary['Quoted'] = dictionary['Quoted'] * 100 if dictionary['Quoted'] < 1.1 else dictionary['Quoted']
    dictionary['Referred'] = dictionary['Referred'] * 100 if dictionary['Referred'] < 1.1 else dictionary['Referred']
    dictionary['choise'] =find_highest_values(dictionary)


    return dictionary

Invalid_Call_schema = {
    "Invalid": {"type": "probability"},
    "Quoted:": {"type": "probability"},
    "Referred": {"type": "probability"},
}


schemaArray=[["Call_schema",Invalid_Call_schema]]

outputArry=[["Call_output",checkoutput]]

def RunUSB(call,prompt,ourCall,ouroutput):
    # Initialize the filter
    filter_obj = OllamaJSONFilter()    
    # Example schema
    schema=""
    for x in range(len(schemaArray)):
        if (schemaArray[x][0]==ourCall):
            schema=schemaArray[x][1]
    output=""
    for x in range(len(outputArry)):
        if (outputArry[x][0]==ouroutput):
            output=outputArry[x][1]

    #Generate JSON-only output
    result = filter_obj.generate_json_only(
        model="deepseek-r1:7b",  # Change to your preferred model
        prompt=prompt+call,
        schema=schema,
        max_retries=0
    )
    print("done")
    print(result)
    thereturn=output(result)
    print()
    print()
    print()
    print(thereturn)
    return thereturn

call ="   Thank you for calling Shred Nation. This is Kylie. How can I help you?  Hi, Kylie. My name is Sarah. How are you today?  Im good. How are you?  Pretty good, thanks. I work at Sandry in Greenfield, and were looking for an estimate to shred some stuff.  Okay. How many boxes of documents do you have?  Well, weve got a mixed bag of – were ballparking at 600 boxes of various sizes.  The largest box we have is a medium-duty staples box.  I would say the majority of the boxes are the paper ream boxes or the bankers box.  Okay, perfect.  And whats the zip code?  01301.  All righty. Is that at a business location?  It is.  Okay.  No, were a business. Its in our basement.  Okay. We do have a shredder who services the area, Sarah.  If you want, I can verify your contact information, and then they would be able to contact you back with –  or I can transfer you to them for pricing.  Okay.  Can you tell me who that is?  Yes. So, it is Secure Shreds.  Okay.  And then Ill send you an email with their profile as well, okay?  Yeah, so you can send me to them?  Yeah, Ill transfer you. Can I just verify your info real quick?  Sure.  Okay. Sarah, whats your last name?  Janest. J-E-N-E-S-T.  And then Sarah, S-A-R-E-S-T.  S-A-R-E-H or no H?  No H.  Okay. And then the name of the business, please?  Sandry. S-A-N-D-R-I.  Perfect. And then last thing I need is just your email so I can send you that information.  Sure. Its S as in Sarah, and my last name, so its J-E-N-E-S-T, Sandry.  S-A-N-D-R-I.com.  Perfect. Ill send you an email with their info. Theres a section for you to rate my service at the bottom, if you could. Its appreciated.  And then actually, Sarah, theres a couple of shredders who service the area. Do you need to witness the shredding, or are you okay with a secure pickup and a certificate of destruction?  I think were okay with a secure pickup.  Okay. Ill get you a couple different quotes then, and then you guys can go with the best option. Is that okay?  Perfect.  Okay.  All right. Sounds good.  Well, I hope you have a great rest of your day.  I hope you have a great rest of your day. Theyll be in touch.  Okay. So were done? Youre not transferring me to them?  No, because Im going to get you two different quotes instead of just the one, and then you can pick the cheapest option and the one who gives you the best, you know, available dates as well.  Okay. All right. Appreciate it. Thank you.  All right. Youre welcome.  Take care.  You too. Bye."
prompt="Rate these calls by Invalid  is (SCREEN RATINGS are  Robo or automated call Marketing calls Caller dialed the wrong number or requesting a service we don’t offer (notary) etc) and Quoted is (Customer was quoted an exact presold price for mobile service and Customer)  Referred is  (in our network  ) this is the call be sure to rate with a float make sure not all of them are 0 please make sure to rate right " 
ourcall="Call_schema"
output="Call_output"
def getready(array):
    #return RunUSB(array[0][1],array[0][2],array[0][3],array[0][3])
    return RunUSB(array[0],array[1],array[2],array[3])

#letsgo=RunUSB(call,prompt,ourcall,output)
#print()
#print()
#print(letsgo)
