from sklearn.linear_model import LinearRegression
import numpy as np
from skl2onnx import to_onnx

np.random.seed(0)
X = np.random.rand(100000, 3).astype(np.float32) # 100000 instances, 3 features
y = X.sum(axis=1) # y is the sum of features

reg = LinearRegression()
reg.fit(X, y)

test_samples = np.array([[0.1, 0.1, 0.1], [0.1, 0.2, 0.2], [0.2, 0.2, 0.2]])
predictions = reg.predict(test_samples)
print("Predictions:", predictions)

# Convert into ONNX format
onx = to_onnx(reg, X[:1], target_opset=9) # target_opset is the ONNX version to use
with open("test_regression.onnx", "wb") as f:
    f.write(onx.SerializeToString())
