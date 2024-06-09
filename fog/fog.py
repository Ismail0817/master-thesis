from flask import Flask, request, jsonify
import pandas as pd
from sklearn.preprocessing import StandardScaler

app = Flask(__name__)

@app.route('/preprocess', methods=['POST'])
def preprocess():
    data = request.get_json()
    print(data, flush=True)
    # return jsonify(data)


    # # Convert data to a DataFrame
    df = pd.DataFrame(data)
    print(df, "\n",  flush=True)

    # Drop rows with NaN values
    df = df.dropna()
    print(df, "\n",  flush=True)   

    # Normalize or standardize numerical values
    columns_to_scale = df.columns.difference(['utc', 'Fire Alarm'])
    # Initialize StandardScaler
    scaler = StandardScaler()
    # Apply scaler on the selected columns
    df[columns_to_scale] = scaler.fit_transform(df[columns_to_scale])
    print("Scaled DataFrame:","\n", flush=True)
    print(df,"\n", flush=True)

    # # Return the preprocessed data
    return jsonify(df.to_dict(orient='records'))

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5005)
