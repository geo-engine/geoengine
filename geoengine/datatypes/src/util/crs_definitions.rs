use crs_definitions::{Def, from_code};

use crate::{
    primitives::{AxisAlignedRectangle, Coordinate2D},
    spatial_reference::AreaOfUseProvider,
};

#[derive(Clone, Debug, Copy)]
pub struct StaticAreaofUseProvider {
    def: Def,
}

impl AreaOfUseProvider for StaticAreaofUseProvider {
    fn new_known_crs(spatial_ref: crate::spatial_reference::SpatialReference) -> super::Result<Self>
    where
        Self: Sized,
    {
        match spatial_ref.authority() {
            crate::spatial_reference::SpatialReferenceAuthority::Epsg
                if spatial_ref.code() < u32::from(u16::MAX) =>
            {
                let defn = from_code(spatial_ref.code() as u16)
                    .ok_or(crate::error::Error::ProjStringUnresolvable { spatial_ref })?;
                Ok(StaticAreaofUseProvider { def: defn })
            }
            _ => Err(crate::error::Error::ProjStringUnresolvable { spatial_ref }),
        }
    }

    fn area_of_use<T: AxisAlignedRectangle>(&self) -> super::Result<T> {
        if let Some(aou) = self.def.area_of_use {
            T::from_min_max(
                Coordinate2D::new(aou[0], aou[1]),
                Coordinate2D::new(aou[2], aou[3]),
            )
        } else {
            Err(crate::error::Error::NoAreaOfUseDefined {
                proj_string: format!("EPSG:{}", self.def.code),
            })
        }
    }

    fn area_of_use_projected<T: AxisAlignedRectangle>(&self) -> super::Result<T> {
        if let Some(aou) = self.def.projected_bounds {
            T::from_min_max(
                Coordinate2D::new(aou[0], aou[1]),
                Coordinate2D::new(aou[2], aou[3]),
            )
        } else {
            Err(crate::error::Error::NoAreaOfUseDefined {
                proj_string: format!("EPSG:{}", self.def.code),
            })
        }
    }
}
