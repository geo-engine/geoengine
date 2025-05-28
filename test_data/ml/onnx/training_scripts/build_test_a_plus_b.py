from onnx import TensorProto, OperatorSetIdProto
from onnx.helper import (
    make_model, make_node, make_graph,
    make_tensor_value_info)
from onnx.checker import check_model


# Inputs, 'X' is the name, TensorProto.FLOAT the type, [None, 512, 512, 2] the shape
X = make_tensor_value_info('X', TensorProto.FLOAT, [None, 512, 512, 2])

# outputs, the shape is [None, 512, 512] which shuld be the same as [None, 512, 512, 1]
Y = make_tensor_value_info('Y', TensorProto.FLOAT, [None, 512, 512])

# operators

# we need to split the input which consists of tuples [band 0, band 1]
split = make_node(
    "Split",
    inputs=["X"],
    outputs=["A_2", "A_1"],
    axis=-1,
    num_outputs=2
)

# now we can use the splitted inputs to calculate something

add = make_node('Add', ['A_1', 'A_2'], ['Y'])

# create a graph from the operators
graph = make_graph([split, add], 'a_plus_b', [X], [Y])
# generate the model from the graph for a specific opset
opset = OperatorSetIdProto(version=21)
model = make_model(graph, opset_imports=[opset])

# check the model
check_model(model)

with open("a_plus_b.onnx", "wb") as text_file:
    text_file.write(model.SerializeToString())
