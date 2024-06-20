from sklearn.tree import DecisionTreeClassifier
import numpy as np

np.random.seed(0) 
X = np.random.rand(100, 2).astype(np.float32)  # 100 instances, 2 features
y = np.where(X[:, 0] > X[:, 1], 42, 33)  # 1 if feature 0 > feature 42, else 33

clf = DecisionTreeClassifier()
clf.fit(X, y)

test_samples = np.array([[0.1, 0.2], [0.2, 0.1]])
predictions = clf.predict(test_samples)
print("Predictions:", predictions)

# Convert into ONNX format.
from skl2onnx import to_onnx

onx = to_onnx(clf, X[:1], target_opset=9) # target_opset is the ONNX version to use
with open("test_classification.onnx", "wb") as f:
    f.write(onx.SerializeToString())
