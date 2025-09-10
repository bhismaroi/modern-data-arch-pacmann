#!/usr/bin/env python3
"""
Standalone model training script for sales prediction
Can be run independently or called from Airflow
"""

import pandas as pd
import numpy as np
from sklearn.model_selection import train_test_split, cross_val_score
from sklearn.linear_model import LinearRegression
from sklearn.ensemble import RandomForestRegressor, GradientBoostingRegressor
from sklearn.preprocessing import LabelEncoder, StandardScaler
from sklearn.metrics import mean_squared_error, r2_score, mean_absolute_error
import joblib
import json
from datetime import datetime
import os
import argparse
import warnings
warnings.filterwarnings('ignore')

class SalesPredictionModel:
    """
    Sales prediction model with preprocessing and training capabilities
    """
    
    def __init__(self, model_type='linear_regression'):
        """
        Initialize the model
        
        Args:
            model_type: Type of model to use ('linear_regression', 'random_forest', 'gradient_boosting')
        """
        self.model_type = model_type
        self.model = None
        self.scaler = StandardScaler()
        self.label_encoders = {}
        self.feature_columns = None
        self.metrics = {}
        
    def load_data(self, filepath=None, df=None):
        """
        Load data from file or DataFrame
        """
        if filepath:
            self.df = pd.read_csv(filepath)
        elif df is not None:
            self.df = df
        else:
            raise ValueError("Either filepath or df must be provided")
        
        print(f"Loaded {len(self.df)} records")
        return self.df
    
    def prepare_features(self, df=None):
        """
        Prepare features for model training
        """
        if df is None:
            df = self.df.copy()
        else:
            df = df.copy()
        
        # Convert date to datetime if it's a string
        if 'transaction_date' in df.columns:
            df['transaction_date'] = pd.to_datetime(df['transaction_date'])
            
            # Extract date features
            df['day_of_week'] = df['transaction_date'].dt.dayofweek
            df['month'] = df['transaction_date'].dt.month
            df['quarter'] = df['transaction_date'].dt.quarter
            df['is_weekend'] = (df['day_of_week'] >= 5).astype(int)
            df['day_of_month'] = df['transaction_date'].dt.day
            df['week_of_year'] = df['transaction_date'].dt.isocalendar().week
        
        # Calculate additional features
        if 'unit_price' in df.columns and 'quantity' in df.columns:
            df['total_before_discount'] = df['unit_price'] * df['quantity']
            
        if 'discount_percentage' in df.columns:
            df['has_discount'] = (df['discount_percentage'] > 0).astype(int)
            df['discount_category'] = pd.cut(df['discount_percentage'], 
                                            bins=[-1, 0, 10, 20, 100],
                                            labels=['none', 'low', 'medium', 'high'])
        
        # Encode categorical variables
        categorical_columns = ['category', 'subcategory', 'customer_segment', 
                              'region', 'payment_method', 'city']
        
        for col in categorical_columns:
            if col in df.columns:
                if col not in self.label_encoders:
                    self.label_encoders[col] = LabelEncoder()
                    df[f'{col}_encoded'] = self.label_encoders[col].fit_transform(df[col])
                else:
                    # Handle unseen categories
                    df[f'{col}_encoded'] = df[col].apply(
                        lambda x: self.label_encoders[col].transform([x])[0] 
                        if x in self.label_encoders[col].classes_ else -1
                    )
        
        # Encode discount_category if it exists
        if 'discount_category' in df.columns:
            if 'discount_category' not in self.label_encoders:
                self.label_encoders['discount_category'] = LabelEncoder()
                df['discount_category_encoded'] = self.label_encoders['discount_category'].fit_transform(df['discount_category'])
            else:
                df['discount_category_encoded'] = df['discount_category'].apply(
                    lambda x: self.label_encoders['discount_category'].transform([x])[0]
                    if x in self.label_encoders['discount_category'].classes_ else -1
                )
        
        # Create interaction features
        if 'quantity' in df.columns and 'unit_price' in df.columns:
            df['quantity_price_interaction'] = df['quantity'] * df['unit_price']
        
        if 'discount_percentage' in df.columns and 'customer_segment_encoded' in df.columns:
            df['discount_segment_interaction'] = df['discount_percentage'] * df['customer_segment_encoded']
        
        # Select features
        self.feature_columns = [
            'quantity', 'unit_price', 'discount_percentage',
            'category_encoded', 'subcategory_encoded', 
            'customer_segment_encoded', 'region_encoded', 
            'payment_method_encoded', 'city_encoded',
            'day_of_week', 'month', 'quarter', 'is_weekend',
            'day_of_month', 'week_of_year',
            'has_discount', 'discount_category_encoded',
            'quantity_price_interaction', 'discount_segment_interaction'
        ]
        
        # Keep only available features
        self.feature_columns = [col for col in self.feature_columns if col in df.columns]
        
        return df
    
    def train(self, test_size=0.2, random_state=42):
        """
        Train the model
        """
        # Prepare features
        df_processed = self.prepare_features()
        
        # Prepare X and y
        X = df_processed[self.feature_columns]
        y = df_processed['total_amount']
        
        # Split data
        X_train, X_test, y_train, y_test = train_test_split(
            X, y, test_size=test_size, random_state=random_state
        )
        
        # Scale features
        X_train_scaled = self.scaler.fit_transform(X_train)
        X_test_scaled = self.scaler.transform(X_test)
        
        # Initialize model based on type
        if self.model_type == 'linear_regression':
            self.model = LinearRegression()
        elif self.model_type == 'random_forest':
            self.model = RandomForestRegressor(
                n_estimators=100,
                max_depth=10,
                random_state=random_state
            )
        elif self.model_type == 'gradient_boosting':
            self.model = GradientBoostingRegressor(
                n_estimators=100,
                learning_rate=0.1,
                max_depth=5,
                random_state=random_state
            )
        else:
            raise ValueError(f"Unknown model type: {self.model_type}")
        
        # Train model
        print(f"Training {self.model_type} model...")
        self.model.fit(X_train_scaled, y_train)
        
        # Make predictions
        y_train_pred = self.model.predict(X_train_scaled)
        y_test_pred = self.model.predict(X_test_scaled)
        
        # Calculate metrics
        self.metrics = {
            'model_type': self.model_type,
            'train_mse': mean_squared_error(y_train, y_train_pred),
            'test_mse': mean_squared_error(y_test, y_test_pred),
            'train_rmse': np.sqrt(mean_squared_error(y_train, y_train_pred)),
            'test_rmse': np.sqrt(mean_squared_error(y_test, y_test_pred)),
            'train_r2': r2_score(y_train, y_train_pred),
            'test_r2': r2_score(y_test, y_test_pred),
            'train_mae': mean_absolute_error(y_train, y_train_pred),
            'test_mae': mean_absolute_error(y_test, y_test_pred),
            'training_samples': len(X_train),
            'test_samples': len(X_test),
            'features_used': self.feature_columns,
            'training_date': datetime.now().isoformat()
        }
        
        # Perform cross-validation
        if self.model_type == 'linear_regression':
            cv_scores = cross_val_score(self.model, X_train_scaled, y_train, 
                                       cv=5, scoring='r2')
            self.metrics['cv_r2_mean'] = cv_scores.mean()
            self.metrics['cv_r2_std'] = cv_scores.std()
        
        # Feature importance for tree-based models
        if hasattr(self.model, 'feature_importances_'):
            feature_importance = dict(zip(self.feature_columns, 
                                         self.model.feature_importances_))
            # Get top 10 important features
            top_features = dict(sorted(feature_importance.items(), 
                                     key=lambda x: x[1], 
                                     reverse=True)[:10])
            self.metrics['top_features'] = top_features
        
        self.print_metrics()
        return self.metrics
    
    def print_metrics(self):
        """
        Print model metrics
        """
        print("\n" + "="*50)
        print(f"Model Performance ({self.model_type})")
        print("="*50)
        print(f"Training Samples: {self.metrics['training_samples']}")
        print(f"Test Samples: {self.metrics['test_samples']}")
        print(f"\nTraining Metrics:")
        print(f"  R² Score: {self.metrics['train_r2']:.4f}")
        print(f"  RMSE: ${self.metrics['train_rmse']:.2f}")
        print(f"  MAE: ${self.metrics['train_mae']:.2f}")
        print(f"\nTest Metrics:")
        print(f"  R² Score: {self.metrics['test_r2']:.4f}")
        print(f"  RMSE: ${self.metrics['test_rmse']:.2f}")
        print(f"  MAE: ${self.metrics['test_mae']:.2f}")
        
        if 'cv_r2_mean' in self.metrics:
            print(f"\nCross-Validation R² Score: {self.metrics['cv_r2_mean']:.4f} (+/- {self.metrics['cv_r2_std']:.4f})")
        
        if 'top_features' in self.metrics:
            print("\nTop 10 Important Features:")
            for feature, importance in self.metrics['top_features'].items():
                print(f"  {feature}: {importance:.4f}")
    
    def save_model(self, filepath='models/sales_predictor.joblib'):
        """
        Save the trained model
        """
        os.makedirs(os.path.dirname(filepath), exist_ok=True)
        
        model_package = {
            'model': self.model,
            'scaler': self.scaler,
            'label_encoders': self.label_encoders,
            'feature_columns': self.feature_columns,
            'metrics': self.metrics,
            'model_type': self.model_type,
            'created_at': datetime.now().isoformat()
        }
        
        joblib.dump(model_package, filepath)
        print(f"\nModel saved to: {filepath}")
        
        # Save metrics separately
        metrics_filepath = filepath.replace('.joblib', '_metrics.json')
        with open(metrics_filepath, 'w') as f:
            # Convert numpy types to Python types for JSON serialization
            metrics_json = {}
            for key, value in self.metrics.items():
                if isinstance(value, np.ndarray):
                    metrics_json[key] = value.tolist()
                elif isinstance(value, (np.float32, np.float64)):
                    metrics_json[key] = float(value)
                elif isinstance(value, (np.int32, np.int64)):
                    metrics_json[key] = int(value)
                else:
                    metrics_json[key] = value
            json.dump(metrics_json, f, indent=2)
        print(f"Metrics saved to: {metrics_filepath}")
    
    def load_model(self, filepath):
        """
        Load a saved model
        """
        model_package = joblib.load(filepath)
        self.model = model_package['model']
        self.scaler = model_package['scaler']
        self.label_encoders = model_package['label_encoders']
        self.feature_columns = model_package['feature_columns']
        self.metrics = model_package.get('metrics', {})
        self.model_type = model_package.get('model_type', 'unknown')
        print(f"Model loaded from: {filepath}")
    
    def predict(self, df):
        """
        Make predictions on new data
        """
        if self.model is None:
            raise ValueError("Model not trained or loaded")
        
        # Prepare features
        df_processed = self.prepare_features(df)
        X = df_processed[self.feature_columns]
        
        # Scale features
        X_scaled = self.scaler.transform(X)
        
        # Make predictions
        predictions = self.model.predict(X_scaled)
        
        return predictions

def main():
    parser = argparse.ArgumentParser(description='Train sales prediction model')
    parser.add_argument('--data', type=str, default='data/sample_sales.csv',
                        help='Path to training data CSV')
    parser.add_argument('--model-type', type=str, default='linear_regression',
                        choices=['linear_regression', 'random_forest', 'gradient_boosting'],
                        help='Type of model to train')
    parser.add_argument('--output', type=str, default='models/sales_predictor.joblib',
                        help='Output path for trained model')
    parser.add_argument('--test-size', type=float, default=0.2,
                        help='Test set size (default: 0.2)')
    
    args = parser.parse_args()
    
    # Create and train model
    model = SalesPredictionModel(model_type=args.model_type)
    model.load_data(filepath=args.data)
    model.train(test_size=args.test_size)
    model.save_model(filepath=args.output)

if __name__ == "__main__":
    main()