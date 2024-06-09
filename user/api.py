from flask import Flask, request

app = Flask(__name__)

@app.route('/api', methods=['POST'])
def receive_message():
    message = request.get_json()
    print(message)
    return 'Message received in user API\n'

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5003)