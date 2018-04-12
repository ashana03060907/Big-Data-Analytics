import json

def product(event, context):
    ##For DEBUGGING purpose. Check logs at CloudWatch -> Logs ##
    
    # for k in event.keys():
    #     print(k, type(k), event[k])
    # evt1 = event["queryStringParameters"] # For GET
    # print(event.keys())
    
    ## Error handling ##   
    # Case-1: if post body input is not in json format
    try:
        evt = json.loads(event["body"]) # For POST body needs to be in JSON format
    except Exception:
        return dict(
            statusCode=500,
            body= "please provide the input in json key-value  format"
        )
    
    # Case-2: If key names are not num1 and num2
    try:
        if evt["num1"] and evt["num2"] in evt.keys():
            pass
    except Exception:
        return dict(
        statusCode=500,
        body= "expecting num1 and num2 as keynames"
    )
    
    # Case-3: If inputs are not numeric
    try:
        if (float(evt["num1"]) or int(evt["num1"])) and (float(evt["num2"]) or int(evt["num2"])):
            pass
    except Exception:
        return dict(
            statusCode=500,
            body= "expecting numeric values as inputs"
        )
    
    ## Calculating the product of two numbers
    try: 
        # Works for GET method and query params
        # eg: https://vftfwq81p0.execute-api.us-east-2.amazonaws.com/dev/echo/product?num1=2.2&num2=3.1   
    
        # Works for POST. Use postman, provide authorization: AWS Signature, provide body: raw, application/json
        body = {
            'num1' : float(evt["num1"]),
            'num2' : float(evt["num2"]),
            'product' : float(evt["num1"])*float(evt["num2"])
        }

        response = {
            "statusCode": 200,
            "body": json.dumps(body)
        }
    
        return response
    
    except Exception as e:
        return dict(
            statusCode=500,
            body=repr(e)
        )
        
    
    
        
    
