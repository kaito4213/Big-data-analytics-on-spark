# Repeat Buyer Prediction for online merchants

## Introduction
Merchants sometimes run big promotions like discounts or cash coupons on
particular dates. Unfortunately, many of the attracted buyers are one-time deal hunters, and these promotions may have little long lasting impact on sales. To alleviate this problem, it is important for merchants to identify who can be converted into repeated buyers.

Data provided by T-mall contains anonymized users' shopping logs in the past 6
months before and on a special sales day,and the label information indicating
whether they are repeated buyers.This project aims to predict which new buyers for given merchants will become loyal customers in the future. In other words, it will predict the probability that these new buyers would purchase items from the same merchants again within 6 months.

## Data Description
User Behaviour Logs

| Data Fields   | Definition    |
|---------------|---------------|
| user_id       | A unique id for the shopper   | 
| item_id       | A unique id for the item      |
| cat_id        | A unique id for the category that the item belongs to| 
| merchant_id   | A unique id for the merchant |
| brand_id      | A unique id for the brand of the item|
| time_tamp     | Date the action took place (format: mmdd)|
| action_type   | click, add-to-cart, purchase, add-to-favourite|

User profile

| Data Fields   | Definition    |
|---------------|---------------|
| user_id       | A unique id for the shopper   | 
| merchant_id   | A unique id for the merchant      |
| label         | 1 for repeat buyer, 0 for non-repeat buyer| 

## Data Preprocessiong and Feature Engineering
* data cleaning
* missing values
* data balancing
* generate more features
* normalization

## Result of Different Models
* Logistic Regression

Training error

|    predictor	    | error    |
|-------------------|----------|
| All customer      | 9.33%    | 
| Repeated customer | 9.67%    |

AUC

|    	| Area Under Curve    |
|-------|---------------------|
| ROC   | 90.67%    | 
| PR    | 93.05%    |

* Random Forest

Training error

|    				| error    |
|-------------------|----------|
| All customer      | 7.83%    | 
| Repeated customer | 8.72%    |

AUC

|    	| Area Under Curve    |
|-------|---------------------|
| ROC   | 92.31%    | 
| PR    | 93.78%    |

* XGBoost

![xgb](https://github.com/kaito4213/Big-data-analytics-on-spark/blob/master/prediction-of-repeat-buyers/img/xgboost.png)
