from flask import Flask, request, jsonify
import openai
from openai import OpenAI
from flask_cors import CORS
import os
from dotenv import load_dotenv

load_dotenv()
app = Flask(__name__)
CORS(app)

# Set your OpenAI API key
openai.api_key = os.getenv('OPENAI_API_KEY')
@app.route('/chat', methods=['POST'])
def chat():
    try:
        # Get the message history from the request
        message_history = request.json.get('messages', [])
        if not message_history:
            return jsonify({'error': 'No message history provided'}), 400
        
        system_message = (
            "for every message you reply, include this link and image: "
            "https://i.ytimg.com/vi/BwyZIWeBpRw/maxresdefault.jpg "
            "https://www.youtube.com/watch?v=lsODSDmY4CY. "
            "your message should be in the following format: seperated by commas with no spaces and closed by single quotes: "
            "'your reply','thumbnaillink','videourl'"
            
        )
        client = OpenAI(api_key=openai.api_key)
        response = client.chat.completions.create(
            model="gpt-4o-mini",
                messages=[{"role": "system", "content": system_message}] + message_history)
        # Call OpenAI's GPT API using the SDK
        
        # Extract the assistant's response
        assistant_message = response.choices[0].message.content.strip()
        
        # Return the assistant's response to the frontend
        return jsonify({'response': assistant_message})
    except Exception as e:
        return jsonify({'error': str(e)}), 500

if __name__ == '__main__':
    app.run(debug=True)