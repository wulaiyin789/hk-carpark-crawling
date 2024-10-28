interface VehicleType {
  type: string;
  service_category: ServiceCategory[];
}

interface ServiceCategory {
  category: string;
  vacancy_type: string;
  vacancy: number;
  lastupdate: string;
}

interface ParkInfo {
  park_id: string;
  vehicle_type: VehicleType[];
  name_en?: string;
  name_tc?: string;
  name_sc?: string;
  displayAddress_en?: string;
  displayAddress_tc?: string;
  displayAddress_sc?: string;
  latitude?: number;
  longitude?: number;
  district_en?: string;
  district_tc?: string;
  district_sc?: string;
  contactNo?: string;
  opening_status?: string;
  height?: number;
  remark_en?: string;
  remark_tc?: string;
  remark_sc?: string;
  website_en?: string;
  website_tc?: string;
  website_sc?: string;
  carpark_photo?: string;
  carpark_info_vacancy?: any
}