import openai
import numpy as np

from api_secrets import *

openai.api_key = OPEN_AI_SECRET_KEY

tweet = """
#SPX 1HR 4000 level has been rejected again. 
Break and close below 3975 and we will go lower, and #crypto markets will Pullback lower.  
"""
prompt = f"from a scale from 0 to  100 give the confidence of this tweet  the stock price will rise: {tweet}"
prompt = f"Decide whether a Tweet's sentiment is positive, neutral, or negative : {tweet}"

"gpt-3.5-turbo"

completions  = openai.Completion.create(
    engine="text-davinci-003",
    prompt=prompt,
    max_tokens=100,
    n=10,
    stop=None,
    temperature=0.5,
    logprobs=2
)

res = []
for c in completions.choices :
    print(c.text)

    res.append(int(c.text))
print(f'mean confidence: {np.mean(res)}')