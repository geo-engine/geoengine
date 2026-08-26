"""Build a tiny constant-output ONNX object-detection model for the
``OnnxObjectDetection`` tests.

The operator feeds the model an NHWC tensor ``[1, H, W, C]`` and reads back a
flat detection tensor. Instead of a real detector network we emit a fixed
channel-major ``[C_total, N]`` output that does *not* depend on the input:
  - ``C_total = 6``: 4 box channels (xc, yc, w, h) + 2 class scores (YOLOv8-style,
    no objectness channel)
  - ``N = 4`` candidates

The constant encodes two strong, non-overlapping detections (class 0 score 0.9
and class 1 score 0.85) plus two weak candidates (score 0.1) that must be
filtered out by the confidence threshold:

  channel            cand0   cand1   cand2   cand3
  xc (0)            1.0     2.5     1.0     1.0
  yc (1)            1.0     2.5     1.0     1.0
  w  (2)            1.0     1.0     1.0     1.0
  h  (3)            1.0     1.0     1.0     1.0
  class0 (4)        0.90    0.05    0.10    0.10
  class1 (5)        0.05    0.85    0.05    0.05

The graph input (``input``) is declared but unused so the session exposes a
named input the operator can bind; the output is produced from an initializer
via an ``Identity`` node.

The file is written to the current working directory; the sibling ``.onnx`` test
models live in ``test_data/ml/onnx/``, so run from there:
    cd test_data/ml/onnx && python training_scripts/build_test_detection.py
"""

import numpy as np
from onnx import TensorProto, helper, numpy_helper


def build():
    constant = numpy_helper.from_array(
        np.array(
            [
                [1.0, 2.5, 1.0, 1.0],   # xc
                [1.0, 2.5, 1.0, 1.0],   # yc
                [1.0, 1.0, 1.0, 1.0],   # w
                [1.0, 1.0, 1.0, 1.0],   # h
                [0.90, 0.05, 0.10, 0.10],  # class 0
                [0.05, 0.85, 0.05, 0.05],  # class 1
            ],
            dtype=np.float32,
        ),
        name="const_output",
    )

    nodes = [helper.make_node("Identity", ["const_output"], ["output"])]

    graph = helper.make_graph(
        nodes,
        "test_detection",
        inputs=[
            helper.make_tensor_value_info("input", TensorProto.FLOAT, [1, 4, 4, 1])
        ],
        outputs=[
            helper.make_tensor_value_info("output", TensorProto.FLOAT, [6, 4])
        ],
        initializer=[constant],
    )

    return helper.make_model(
        graph, opset_imports=[helper.make_opsetid("", 13)], ir_version=8
    )


if __name__ == "__main__":
    m = build()
    with open("test_detection.onnx", "wb") as f:
        f.write(m.SerializeToString())
    print("wrote test_detection.onnx")
