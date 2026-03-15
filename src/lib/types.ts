export interface State {
  abbr: string;
  name: string;
  fips: string | null;
  hospital_count: number;
  provider_count: number;
  procedure_count: number;
  total_services: number;
  avg_medicare_payment: number | null;
  avg_submitted_charge: number | null;
}

export interface Hospital {
  facility_id: string;
  name: string;
  slug: string;
  address: string | null;
  city: string;
  state: string;
  zip: string | null;
  county: string | null;
  phone: string | null;
  hospital_type: string | null;
  ownership: string | null;
  emergency_services: number;
  overall_rating: number | null;
  mortality_better: number;
  mortality_same: number;
  mortality_worse: number;
  safety_better: number;
  safety_same: number;
  safety_worse: number;
  readmission_better: number;
  readmission_same: number;
  readmission_worse: number;
  birthing_friendly: number;
}

export interface Procedure {
  code: string;
  description: string;
  slug: string;
  category: string;
  is_drug: number;
  place_of_service: string | null;
  national_avg_medicare_payment: number | null;
  national_avg_submitted_charge: number | null;
  national_avg_allowed_amount: number | null;
  national_total_services: number | null;
  national_total_beneficiaries: number | null;
  national_provider_count: number | null;
  state_count: number;
  price_range_low: number | null;
  price_range_high: number | null;
}

export interface ProcedureCategory {
  slug: string;
  name: string;
  procedure_count: number;
  total_services: number;
  avg_medicare_payment: number | null;
  description: string | null;
}

export interface ProcedureStatePrice {
  procedure_code: string;
  state: string;
  avg_medicare_payment: number | null;
  avg_submitted_charge: number | null;
  avg_allowed_amount: number | null;
  min_payment: number | null;
  max_payment: number | null;
  total_services: number | null;
  total_beneficiaries: number | null;
  provider_count: number | null;
}

export interface Provider {
  npi: string;
  name: string;
  slug: string;
  first_name: string | null;
  credentials: string | null;
  entity_type: string | null;
  address: string | null;
  city: string | null;
  state: string | null;
  zip: string | null;
  provider_type: string | null;
  total_services: number | null;
  total_beneficiaries: number | null;
  total_medicare_payment: number | null;
  total_submitted_charge: number | null;
  procedure_count: number;
}

export interface ProviderProcedure {
  npi: string;
  procedure_code: string;
  total_services: number | null;
  total_beneficiaries: number | null;
  avg_medicare_payment: number | null;
  avg_submitted_charge: number | null;
  avg_allowed_amount: number | null;
  place_of_service: string | null;
}

export interface Stat {
  key: string;
  value: string;
}

// Extended types for joins
export interface ProcedureStatePriceWithState extends ProcedureStatePrice {
  state_name: string;
}

export interface ProviderProcedureWithDetails extends ProviderProcedure {
  provider_name: string;
  provider_slug: string;
  provider_city: string | null;
  provider_state: string | null;
  provider_type: string | null;
  credentials: string | null;
}

export interface HospitalProcedure {
  procedure_code: string;
  description: string;
  slug: string;
  category: string;
  total_services: number;
  avg_medicare_payment: number;
  avg_submitted_charge: number;
}
