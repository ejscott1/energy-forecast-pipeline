import os
import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine
from urllib.parse import quote_plus

# New imports for Machine Learning
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestRegressor
from sklearn.metrics import mean_absolute_error
import joblib


# Load environment variables
load_dotenv()


def get_sql_engine():
    """Establishes a SQLAlchemy engine connection to the PostgreSQL database."""
    try:
        db_password = os.getenv("DB_PASSWORD")
        if not db_password:
            raise ValueError("DB_PASSWORD not found in .env file!")

        safe_password = quote_plus(db_password)
        db_url = f"postgresql://postgres:{safe_password}" "@localhost:5432/energy_db"
        engine = create_engine(db_url)
        return engine
    except Exception as e:
        print(f"Could not create database engine: {e}")
        return None


def load_features_data(engine):
    """Loads all data from the features_demand table into a Pandas DataFrame."""
    print("Loading data from 'features_demand' table...")
    try:
        # We want to make sure the data is sorted by time
        sql_query = "SELECT * FROM features_demand ORDER BY timestamp;"
        df = pd.read_sql_query(sql_query, engine, index_col="timestamp")

        print(f"Successfully loaded {len(df)} records.")
        return df
    except Exception as e:
        print(f"Error loading data: {e}")
        return None


if __name__ == "__main__":
    engine = get_sql_engine()
    if engine:
        # 1. Load the features data
        features_df = load_features_data(engine)

        if features_df is not None and not features_df.empty:
            print("\n--- Feature data successfully loaded. ---")

            # 2. Define Features (X) and Target (y)
            # The target is what we want to predict: 'demand_mwh'
            # Features are all the columns we'll use to make the prediction.
            target = "demand_mwh"

            # We must drop the target column from our features
            # 'axis=1' means we are dropping a column
            features = features_df.drop(target, axis=1)

            X = features
            y = features_df[target]

            # 3. Split data into Training and Testing sets
            # We use 20% of the data for testing.
            # We MUST set shuffle=False for time-series data!
            X_train, X_test, y_train, y_test = train_test_split(
                X, y, test_size=0.2, shuffle=False
            )

            print(f"Training data shape: {X_train.shape}")
            print(f"Testing data shape: {X_test.shape}")

            # 4. Train a Model
            print("\nTraining RandomForestRegressor model...")
            # We use 'n_estimators=100' (100 trees) as a good baseline.
            # 'random_state=42' ensures our model is reproducible.
            model = RandomForestRegressor(n_estimators=100, random_state=42, n_jobs=-1)
            model.fit(X_train, y_train)
            print("Model training complete.")

            # 5. Evaluate the Model
            print("\nEvaluating model...")
            predictions = model.predict(X_test)
            mae = mean_absolute_error(y_test, predictions)

            # Also show the R-squared score (how much of the variance is explained)
            r2 = model.score(X_test, y_test)

            print(f"Model Mean Absolute Error (MAE): {mae:,.0f}")
            print(f"Model R-squared (R2) score: {r2:.2f}")

            # 6. Save (serialize) the trained model
            model_filename = "demand_forecaster.joblib"
            print(f"\nSaving model to {model_filename}...")
            joblib.dump(model, model_filename)
            print("Model saved successfully.")

        else:
            print("Feature data is empty. Exiting.")
