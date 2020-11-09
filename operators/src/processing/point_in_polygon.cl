__kernel void add(
        __constant const double2 *IN_POINT_COORDS0,
        __constant const int *IN_POINT_OFFSETS0,
        __constant const double2 *IN_POINT_COORDS1,
        __constant const int *IN_POINT_OFFSETS1,
        __global double2 *OUT_POINT_COORDS0,
        __global int *OUT_POINT_OFFSETS0
) {
    const uint idx = get_global_id(0);
    printf("%d\n", idx);
}