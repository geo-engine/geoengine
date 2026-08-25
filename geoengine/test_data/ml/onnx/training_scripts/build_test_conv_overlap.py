"""Build a tiny convolution-like ONNX model for the tile-overlap tests.

The operator feeds the model an NHWC tensor ``[1, H, W, 1]`` and reads back a
flat prediction tensor. To exercise *overlap* we need a model whose input pixel
extent (``H x W``) is larger than the output extent: here input ``4x4`` (a 2x2
core padded by a 1px halo) -> output ``2x2``.

Instead of a real ``Conv`` (which is NCHW-only) we build the equivalent with
plain ONNX ops on the NHWC layout:
  Reshape(1,4,4,1) -> (4,4)
  MatMul(ones(4,1)) -> (4,1)  # sum of each input row
  Reshape(4,1) -> (2,2)

So ``out[y][x] = sum of input row``: out[0] uses input rows 0,1 and out[1] uses
input rows 2,3. Rows 0 and 3 are *halo* rows, so the output provably depends on
pixels outside the 2x2 core -- which is exactly what tile-overlap is for.

The file is written to the current working directory; the sibling ``.onnx`` test
models live in ``test_data/ml/onnx/``, so run from there:
    cd test_data/ml/onnx && python training_scripts/build_test_conv_overlap.py
"""

import numpy as np
from onnx import TensorProto, helper, numpy_helper


def build():
    shape_44 = numpy_helper.from_array(np.array([4, 4], dtype=np.int64), name="shape_44")
    shape_22 = numpy_helper.from_array(np.array([2, 2], dtype=np.int64), name="shape_22")
    ones_col = numpy_helper.from_array(np.ones((4, 1), dtype=np.float32), name="ones_col")

    nodes = [
        helper.make_node("Reshape", ["input", "shape_44"], ["r44"]),
        helper.make_node("MatMul", ["r44", "ones_col"], ["row_sums"]),
        helper.make_node("Reshape", ["row_sums", "shape_22"], ["prediction"]),
    ]

    graph = helper.make_graph(
        nodes,
        "test_conv_overlap",
        inputs=[
            helper.make_tensor_value_info("input", TensorProto.FLOAT, [1, 4, 4, 1])
        ],
        outputs=[
            helper.make_tensor_value_info("prediction", TensorProto.FLOAT, [2, 2])
        ],
        initializer=[shape_44, shape_22, ones_col],
    )

    return helper.make_model(
        graph, opset_imports=[helper.make_opsetid("", 13)], ir_version=8
    )


if __name__ == "__main__":
    m = build()
    with open("test_conv_overlap.onnx", "wb") as f:
        f.write(m.SerializeToString())
    print("wrote test_conv_overlap.onnx")
