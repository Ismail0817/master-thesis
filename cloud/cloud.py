from flask import Flask, request, jsonify
import pandas as pd
from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import GridSearchCV, train_test_split
from sklearn.svm import SVC


app = Flask(__name__)


@app.route('/train', methods=['POST'])
def preprocess():
    data = request.get_json()
    print(data, flush=True)
    # return jsonify(data)


    # # Convert data to a DataFrame
    df = pd.DataFrame(data)
    print(df, "\n",  flush=True)

    # Separate features and target
    X = df.drop(columns=['Fire Alarm', 'utc'])
    y = df['Fire Alarm']

    # Split data into training and test sets
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

    # Define parameter grid for GridSearchCV
    param_grid = {
        'RandomForestClassifier': {
            'n_estimators': [10, 50, 100],
            'max_depth': [None, 10, 20, 30]
        },
        'SVC': {
            'C': [0.1, 1, 10],
            'kernel': ['linear', 'rbf']
        }
    }

    # Create classifiers
    classifiers = {
        'RandomForestClassifier': RandomForestClassifier(),
        'SVC': SVC()
    }

    best_estimators = {}
    best_scores = {}

    # GridSearch for each classifier
    for clf_name, clf in classifiers.items():
        print(f"Running GridSearchCV for {clf_name}")
        grid_search = GridSearchCV(clf, param_grid[clf_name], cv=5, scoring='accuracy')
        grid_search.fit(X_train, y_train)
        best_estimators[clf_name] = grid_search.best_estimator_
        best_scores[clf_name] = grid_search.best_score_

    # Print best scores and parameters for each classifier
    for clf_name in best_estimators:
        print(f"Best estimator for {clf_name}: {best_estimators[clf_name]}")
        print(f"Best score for {clf_name}: {best_scores[clf_name]}")

    # Prepare the response
    response = {
        "best_estimators": {clf_name: str(best_estimators[clf_name]) for clf_name in best_estimators},
        "best_scores": best_scores
    }

    return jsonify(response), 200


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5005)
