use crate::primitives::Coordinate2D;

// coordinates used for the tests in EPSG:4326
// and reprojected with proj cs2cs to EPSG:900913
//
// cs2cs -d 10  EPSG:4326 EPSG:900913
// 50.8021728 8.7667933
// 975914.9660458824       6586374.7028446598 0.0000000000
// 50.937531 6.9602786
// 774814.6695313191       6610251.1099264193 0.0000000000
// 53.565278 10.001389
// 1113349.5307054475      7088251.2962248782 0.0000000000

pub const MARBURG_EPSG_4326: Coordinate2D = Coordinate2D {
    x: 8.766_793_3,
    y: 50.802_172_8,
};

pub const MARBURG_EPSG_900_913: Coordinate2D = Coordinate2D {
    x: 975_914.966_045_882_4,
    y: 6_586_374.702_844_657,
};

pub const COLOGNE_EPSG_4326: Coordinate2D = Coordinate2D {
    x: 6.960_278_6,
    y: 50.937_531,
};

pub const COLOGNE_EPSG_900_913: Coordinate2D = Coordinate2D {
    x: 774_814.669_531_319,
    y: 6_610_251.109_926_421,
};

pub const HAMBURG_EPSG_4326: Coordinate2D = Coordinate2D {
    x: 10.001_389,
    y: 53.565_278,
};

pub const HAMBURG_EPSG_900_913: Coordinate2D = Coordinate2D {
    x: 1_113_349.530_705_447_5,
    y: 7_088_251.296_224_877,
};
