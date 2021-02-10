"""Air Two test configuration."""
from pandas import DataFrame
from pytest import fixture

# from pandera import Check, Column, DataFrameSchema


# @fixture
# def schema() -> DataFrameSchema:
#     """Example DataFrameScheme for extracted DataFrame."""
#     return DataFrameSchema(
#         columns={
#             "NCFormat": Column(
#                 pandas_dtype=str,
#                 checks=Check.isin(
#                     [
#                         "Agriculture",
#                         "Agriculture Total",
#                         "Business",
#                         "Business Total",
#                         "Energy Supply",
#                         "Energy Supply Total",
#                         "Grand Total",
#                         "Industrial processes",
#                         "Industrial processes Total",
#                         "Land use, land use change and forestry",
#                         "Land use, land use change and forestry Total",
#                         "Public",
#                         "Public Total",
#                         "Residential",
#                         "Residential Total",
#                         "Transport",
#                         "Transport Total",
#                         "Waste Management",
#                         "Waste Management Total",
#                     ]
#                 ),
#                 nullable=True,
#             ),
#             "IPCC": Column(
#                 pandas_dtype=str,
#                 checks=Check.isin(
#                     [
#                         "1A1ai_Public_Electricity&Heat_Production",
#                         "1A1b_Petroleum_Refining",
#                         "1A1ci_Manufacture_of_solid_fuels",
#                         "1A1cii_Oil_and_gas_extraction",
#                         "1A1ciii_Other_energy_industries",
#                         "1A2a_Iron_and_steel",
#                         "1A2b_Non-Ferrous_Metals",
#                         "1A2c_Chemicals",
#                         "1A2d_Pulp_Paper_Print",
#                         "1A2e_food_processing_beverages_and_tobacco",
#                         "1A2f_Non-metallic_minerals",
#                         "1A2gvii_Off-road_vehicles_and_other_machinery",
#                         "1A2gviii_Other_manufacturing_industries_and_construction",  # noqa: B950 - Line length caused by indentation
#                         "1A3a_Domestic_aviation",
#                         "1A3bi_Cars",
#                         "1A3bii_Light_duty_trucks",
#                         "1A3biii_Heavy_duty_trucks_and_buses",
#                         "1A3biv_Motorcycles",
#                         "1A3bv_Other_road_transport",
#                         "1A3c_Railways",
#                         "1A3d_Domestic_navigation",
#                         "1A3eii_Other_Transportation",
#                         "1A4ai_Commercial/Institutional",
#                         "1A4bi_Residential_stationary",
#                         "1A4bii_Residential:Off-road",
#                         "1A4ci_Agriculture/Forestry/Fishing:Stationary",
#                         "1A4cii_Agriculture/Forestry/Fishing:Off-road",
#                         "1A4ciii_Fishing",
#                         "1A5b_Other:Mobile",
#                         "1B1ai_Underground_mines:Abandoned",
#                         "1B1ai_Underground_mines:Mining_activities",
#                         "1B1ai_Underground_mines:Post-mining_activities",
#                         "1B1aii_Surface_mines:Mining_activities",
#                         "1B1b_Solid_Fuel_Transformation",
#                         "1B2a1_Oil_exploration",
#                         "1B2a2_Oil_Production",
#                         "1B2a3_Oil_transport",
#                         "1B2a4_Oil_refining/storage",
#                         "1B2b1_Gas_exploration",
#                         "1B2b3_Gas_processing",
#                         "1B2b4_Gas_transmission_and_storage",
#                         "1B2b5_Gas_distribution",
#                         "1B2c_Flaring_Gas",
#                         "1B2c_Flaring_Oil",
#                         "1B2c_Venting_Gas",
#                         "1B2c_Venting_Oil",
#                         "2A1_Cement_Production",
#                         "2A2_Lime_Production",
#                         "2A3_Glass_production",
#                         "2A4a_Other_process_uses_of_carbonates:ceramics",
#                         "2A4b_Other_uses_of_Soda_Ash",
#                         "2A4d_Other_process_uses_of_carbonates:other",
#                         "2B1_Ammonia_Production",
#                         "2B1_Chemical_Industry:Ammonia_production",
#                         "2B10_Chemical_Industry:Other",
#                         "2B2_Nitric_Acid_Production",
#                         "2B3_Adipic_Acid_Production",
#                         "2B6_Titanium_dioxide_production",
#                         "2B7_Soda_Ash_Production",
#                         "2B8a_Methanol_production",
#                         "2B8b_Ethylene_Production",
#                         "2B8c_Ethylene_Dichloride_and_Vinyl_Chloride_Monomer",
#                         "2B8d_Ethylene_Oxide",
#                         "2B8e_Acrylonitrile",
#                         "2B8f_Carbon_black_production",
#                         "2B8g_Petrochemical_and_carbon_black_production:Other",
#                         "2B9a1_Fluorchemical_production:By-product_emissions",
#                         "2B9b3_Fluorchemical_production:Fugitive_emissions",
#                         "2C1a_Steel",
#                         "2C1b_Pig_iron",
#                         "2C1d_Sinter",
#                         "2C3_Aluminium_Production",
#                         "2C4_Magnesium_production",
#                         "2C6_Zinc_Production",
#                         "2D1_Lubricant_Use",
#                         "2D2 Non-energy_products_from_fuels_and_solvent_use:Paraffin_wax_use",  # noqa: B950 - Line length caused by indentation
#                         "2D3_Non-energy_products_from_fuels_and_solvent_use:Other",  # noqa: B950 - Line length caused by indentation
#                         "2D4_Other_NEU",
#                         "2E1_Integrated_circuit_or_semiconductor",
#                         "2F1a_Commercial_refrigeration",
#                         "2F1b_Domestic_refrigeration",
#                         "2F1c_Industrial_refrigeration",
#                         "2F1d_Transport_refrigeration",
#                         "2F1e_Mobile_air_conditioning",
#                         "2F1f_Stationary_air_conditioning",
#                         "2F2a_Closed_foam_blowing_agents",
#                         "2F2b_Open_foam_blowing_agents",
#                         "2F3_Fire_Protection",
#                         "2F4a_Metered_dose_inhalers",
#                         "2F4b_Aerosols:Other",
#                         "2F5_Solvents",
#                         "2F6b_Other_Applications:Contained-Refrigerant_containers",  # noqa: B950 - Line length caused by indentation
#                         "2F6b_Other_Applications:Contained-Refrigerant_Processing",  # noqa: B950 - Line length caused by indentation
#                         "2G1_Electrical_equipment",
#                         "2G2_Military_applications",
#                         "2G2_Particle_accelerators",
#                         "2G2e_Electronics_and_shoes",
#                         "2G2e_Tracer_gas",
#                         "2G3a_Medical aplications",
#                         "2G3b_N2O_from_product_uses:_Other",
#                         "2G4_Other_product_manufacture_and_use",
#                         "2G4_Other_product_manufacture_and_use-baking_soda",
#                         "3A1a Enteric Fermentation - dairy cows",
#                         "3A1b Enteric Fermentation - other cattle",
#                         "3A2 Enteric Fermentation - sheep",
#                         "3A3 Enteric Fermentation - swine",
#                         "3A4 Enteric Fermentation - other livestock",
#                         "3B11a Manure management - CH4 - dairy cows",
#                         "3B11b Manure management - CH4 - other cattle",
#                         "3B12 Manure management - CH4 - sheep",
#                         "3B13 Manure management - CH4 - swine",
#                         "3B14 Manure management - CH4 - other livestock",
#                         "3B21a Manure management - N2O and NMVOC - dairy cattle",  # noqa: B950 - Line length caused by indentation
#                         "3B21b Manure management - N2O and NMVOC - other cattle",  # noqa: B950 - Line length caused by indentation
#                         "3B22 Manure management - N2O and NMVOC - sheep",
#                         "3B23 Manure management - N2O and NMVOC -swine ",
#                         "3B24 Manure management - N2O and NMVOC - other livestock",  # noqa: B950 - Line length caused by indentation
#                         "3B25 Manure management - N2O and NMVOC - indirect N2O emissions",  # noqa: B950 - Line length caused by indentation
#                         "3D11 Inorganic N Fertilizers",
#                         "3D12a Animal manure applied to soils",
#                         "3D12b Sewage sludge applied to soils",
#                         "3D13 Urine and Dung deposited by grazing animals",
#                         "3D14 Crop Residues",
#                         "3D15 Mineralisation/immobilisation associated with loss/gain of soil organic matter",  # noqa: B950 - Line length caused by indentation
#                         "3D16 Cultivation of Organic soils",
#                         "3D21 Atmospheric Deposition",
#                         "3D22 Nitrogen Leaching and Run-off",
#                         "3F11_Field_burning",
#                         "3F12_Field_burning",
#                         "3F14_Field_burning",
#                         "3F5_Field_burning",
#                         "3G1_Liming - limestone",
#                         "3G2_Liming - dolomite",
#                         "3H Urea Application",
#                         "4_Indirect_N2O_Emissions",
#                         "4A_Forest Land_Emissions_from_Drainage",
#                         "4A1_ Forest Land remaining Forest Land",
#                         "4A2_1_Cropland converted to Forest Land",
#                         "4A2_2_Grassland converted to Forest Land",
#                         "4A2_4_Settlements converted to Forest Land",
#                         "4A2_5_Other land converted to Forest Land",
#                         "4A2_Cropland converted to Forest Land",
#                         "4A2_Grassland converted to Forest Land",
#                         "4A2_Land converted to Forest Land_Emissions_from_Fertilisation",  # noqa: B950 - Line length caused by indentation
#                         "4A2_Other Land converted to Forest Land",
#                         "4A2_Settlements converted to Forest Land",
#                         "4B1_Cropland Remaining Cropland",
#                         "4B1_Cropland_Remaining_Cropland",
#                         "4B2_1_Forest Land converted to Cropland",
#                         "4B2_2_Grassland converted to Cropland",
#                         "4B2_4_Settlements converted to Cropland",
#                         "4C1_Grassland Remaining Grassland",
#                         "4C2_1_Forest Land converted to Grassland",
#                         "4C2_2_Cropland converted to Grassland",
#                         "4C2_3_Wetlands converted to Grassland",
#                         "4C2_4_Settlements converted to Grassland",
#                         "4D1_Wetlands remaining wetlands",
#                         "4D2_Land converted to wetlands",
#                         "4E1_Settlements remaining settlements",
#                         "4E2_1_Forest Land converted to Settlements",
#                         "4E2_2_Cropland converted to Settlements",
#                         "4E2_3_Grassland converted to Settlements",
#                         "4G_Harvested Wood Products",
#                         "5A1a_Managed_Waste_Disposal_sites_anaerobic",
#                         "5B1a_composting_municipal_solid_waste",
#                         "5B2a_Anaerobic_digestion_municipal_solid_waste",
#                         "5C1.1b_Biogenic:Sewage_sludge",
#                         "5C1.2a_Non-biogenic:municipal_solid_waste",
#                         "5C1.2b_Non-biogenic:Clinical_waste",
#                         "5C1.2b_Non-biogenic:Other_Chemical_waste",
#                         "5C2.2b_Non-biogenic:Other",
#                         "5C2.2b_Non-biogenic:Other_Accidental fires (vehicles)",
#                         "5D1_Domestic_wastewater_treatment",
#                         "5D2_Industrial_wastewater_treatment",
#                     ]
#                 ),
#                 nullable=True,
#             ),
#             "BaseYear": Column(
#                 pandas_dtype=float,
#                 checks=Check.in_range(-10000.0, 650000.0),
#             ),
#             "199[0589]|20[01][0-9]": Column(
#                 pandas_dtype=float,
#                 checks=Check.in_range(-10000.0, 650000.0),
#                 nullable=True,
#                 regex=True,
#             ),
#         },
#         coerce=True,
#         strict=True,
#     )


@fixture
def extracted() -> DataFrame:
    """Example of extracted DataFrame."""
    return DataFrame(
        data={
            "NCFormat": [
                "Agriculture",
                None,
                "Agriculture Total",
            ],
            "IPCC": [
                "1A4ci_Agriculture/Forestry/Fishing:Stationary",
                "1A4cii_Agriculture/Forestry/Fishing:Off-road",
                None,
            ],
            "BaseYear": [
                float(0),
                float(1),
                float(2),
            ],
            "1990": [
                float(3),
                float(4),
                float(5),
            ],
        },
    )


@fixture
def extracted_dropped() -> DataFrame:
    """Example of extracted DataFrame minus BaseYear."""
    return DataFrame(
        data={
            "NCFormat": [
                "Agriculture",
                None,
                "Agriculture Total",
            ],
            "IPCC": [
                "1A4ci_Agriculture/Forestry/Fishing:Stationary",
                "1A4cii_Agriculture/Forestry/Fishing:Off-road",
                None,
            ],
            "1990": [
                float(3),
                float(4),
                float(5),
            ],
        },
    )


@fixture
def extracted_dropped_missing() -> DataFrame:
    """Example of extracted DataFrame minus BaseYear, missing NCFormat[0]."""
    return DataFrame(
        data={
            "NCFormat": [
                None,
                None,
                "Agriculture Total",
            ],
            "IPCC": [
                "1A4ci_Agriculture/Forestry/Fishing:Stationary",
                "1A4cii_Agriculture/Forestry/Fishing:Off-road",
                None,
            ],
            "1990": [
                float(3),
                float(4),
                float(5),
            ],
        },
    )


@fixture
def extracted_filled() -> DataFrame:
    """Example of extracted DataFrame minus BaseYear, NCFormat blanks filled."""
    return DataFrame(
        data={
            "NCFormat": [
                "Agriculture",
                "Agriculture",
                "Agriculture Total",
            ],
            "IPCC": [
                "1A4ci_Agriculture/Forestry/Fishing:Stationary",
                "1A4cii_Agriculture/Forestry/Fishing:Off-road",
                None,
            ],
            "1990": [
                float(3),
                float(4),
                float(5),
            ],
        },
    )


@fixture
def transformed() -> DataFrame:
    """A minimal example of a transformed Air Two DataFrame."""
    return DataFrame(
        data={
            "NCFormat": [
                "Agriculture",
                "Agriculture",
                "Agriculture Total",
            ],
            "IPCC": [
                "1A4ci_Agriculture/Forestry/Fishing:Stationary",
                "1A4cii_Agriculture/Forestry/Fishing:Off-road",
                None,
            ],
            "EmissionYear": [
                "1990",
                "1990",
                "1990",
            ],
            "CO2 Equiv": [
                float(3),
                float(4),
                float(5),
            ],
        },
    )


@fixture
def lookup() -> DataFrame:
    """A minimal example of a look up for the Air Two DataFrame."""
    return DataFrame(
        data={
            "NCFormat": [
                "Agriculture",
                "Agriculture",
            ],
            "IPCC": [
                "1A4ci_Agriculture/Forestry/Fishing:Stationary",
                "1A4cii_Agriculture/Forestry/Fishing:Off-road",
            ],
            "OIF_category": [
                "Agriculture",
                "Agriculture",
            ],
        }
    )


@fixture
def transformed_joined() -> DataFrame:
    """The expected output of joining the transformed and lookup DataFrames."""
    return DataFrame(
        data={
            "NCFormat": [
                "Agriculture",
                "Agriculture",
                "Agriculture Total",
            ],
            "IPCC": [
                "1A4ci_Agriculture/Forestry/Fishing:Stationary",
                "1A4cii_Agriculture/Forestry/Fishing:Off-road",
                None,
            ],
            "EmissionYear": [
                "1990",
                "1990",
                "1990",
            ],
            "CO2 Equiv": [
                float(3),
                float(4),
                float(5),
            ],
            "OIF_category": [
                "Agriculture",
                "Agriculture",
                None,
            ],
        },
    )


@fixture
def enriched() -> DataFrame:
    """A minimal example of an enriched Two DataFrame."""
    return DataFrame(
        data={
            "OIF_category": [
                "Agriculture",
            ],
            "EmissionYear": [
                "1990",
            ],
            "CO2 Equiv": [
                float(7),
            ],
        },
    )
