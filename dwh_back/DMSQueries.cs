namespace VGD.DWHService.Tools
{
    public static class DMSQueries
    {
        public static class Inventory
        {
            public static class Total
            {
                public const string Select = "SELECT * FROM view_get_dwh_inventory  WHERE timestamp_dms >= @lastExecution ORDER BY timestamp_dms asc";
            }

            public static class Incadea
            {
                public const string Select = "SELECT * FROM view_get_dwh_inventory WHERE timestamp_dms >= @lastExecution ORDER BY timestamp_dms asc";             
            }

            public static class Quiter
            {
                public const string Select = "SELECT * FROM view_get_dwh_inventory WHERE timestamp_dms >= @lastExecution";
            }
        }

        public static class Customers
        {
            public static class Total
            {
                public const string Select = @"SELECT *
                        FROM view_get_dwh_customers
						WHERE  timestamp_dms >= @lastExecution
						ORDER BY ndClientDMS";
            }

            public static class Incadea
            {
                public const string Select = @"
                    SELECT     
                        idAgency,
                        ndClientDMS,
                        name,
                        second_name,
                        last_name,
                        bussines_name,
                        rfc,
                        curp,
                        phone,
                        mobile_phone,
                        other_phone,
                        assitant_phone,
                        office_phone,
                        mail,
                        activitie,
                        street,
                        external_number,
                        internal_number,
                        zipcode,
                        between_streets,
                        settlement,
                        deputation,
                        country,
                        city,
                        state,
                        birthay_date,
                        salutation,
                        gender,
                        costumer_type,
                        appointment,
                        allow_contact,
                        ndSeller,
                        seller_Name,
                        clasification,
                        last_sale,
                        idSalesForce,
                        timestamp_dms,
                        timestamp,
                        timestamp_hex
                    FROM DW_Clientes
                    WHERE timestamp_dms >= @lastExecution
                    ORDER BY timestamp_hex asc";
            }

            public static class Quiter
            {
                public const string Select = @"SELECT *
                        FROM view_get_dwh_customers
						WHERE  timestamp_dms >= @lastExecution
						ORDER BY ndClientDMS";
            }
        }

        public static class Invoices
        {
            public static class Total
            {
                public const string Select = "sp_get_invoices";
            }

            public static class Incadea
            {
                public const string Select = @"
                    SELECT 
                        ndPlant as idAgency,
                        order_dms,
                        state,
                        vin,
                        warranty_init_date,
                        plates,
                        payment_method,
                        sub_total,
                        accesories,
                        amount_accesories,
                        financial_terms,
                        invoice_reference,
                        billing_date,
                        amount_taxes,
                        financial_institution,
                        delivery_date,
                        cancelation_date,
                        stage_name,
                        timestamp_dms,
                        timestamp_hex,
                        close_date,
                        description,
                        timestamp,
                        CustomerId AS ndClientDMS,
                        Cliente AS client_bussines_name
                    FROM view_dwh_invoices
                    WHERE timestamp_dms >=@lastExecution";
            }

            public static class Quiter
            {
                public const string Select = @"
                    	SELECT DISTINCT
                        48410047 AS idAgency,
                        PED.REFERENCIA AS order_dms,
                        'Facturado' AS state,
                        VEHI.BASTIDOR AS vin,
                        FAPR.FEC_FACTURA AS warranty_init_date,
                        VEHI.MATRICULA AS plates,
                        PED.DES_TIPO_VENTA_DEST AS payment_method,
                        PED.TOTAL_OFERTA AS sub_total,
                        0 AS accesories,
                        0 AS amount_accesories,
                        '' AS financial_terms,
                        FAPR.NUM_FACTURA AS invoice_reference,
                        FAPR.FEC_FACTURA AS billing_date,
                        FAPR.IMP_IVA AS amount_taxes,
                        '' AS financial_institution,
                        PED.FEC_FINAL AS delivery_date,
                        '' AS cancelation_date,
                        'Facturacion del vehiculo' AS stage_name,
                        VEHI.FEC_ULTIMA_MODIFICACION AS timestamp_dms,
                        CAST(VEHI.FEC_ULTIMA_MODIFICACION AS varbinary) AS timestamp_hex,
                        '' AS close_date,
                        '' AS description,
                        GETDATE() AS timestamp,

                        PED.CLIENTE AS ndClientDMS,
                        COALESCE(NULLIF(CLI.NOMBRE_COMERCIAL, ''), NULLIF(CLI.NOMBRE, ''), NULLIF(CLI.NOMBRE_PERSONAL, ''), '') AS client_bussines_name

                    FROM FMVEHBI_PR AS VEHI
                    INNER JOIN FTOFVEBI_PR AS PED
                        ON VEHI.IDV = PED.IDV
                    INNER JOIN FTVENBI_PR AS FAPR
                        ON PED.IDV = FAPR.IDV
                       AND FAPR.COD_CONCEPTO = 'FF'
                    LEFT JOIN FMCUBI_PR AS CLI
                        ON LTRIM(RTRIM(CLI.CUENTA)) = LTRIM(RTRIM(PED.CLIENTE))
                    WHERE VEHI.FEC_ULTIMA_MODIFICACION >= @lastExecution
                    ORDER BY timestamp_dms ASC";
            }
        }

        public static class Services
        {
            public static class Incadea
            {
                public const string Select = @"SELECT 
                    ndPlant As idAgency,
                    order_dms, 
                    service_date,
                    service_type, 
                    servicer_to_performe AS service_to_perform,
                    km AS kms,
                    CAST(NULL AS NVARCHAR(64)) AS ndClientDMS,
                    CAST(NULL AS NVARCHAR(256)) AS nmVendedor,
                    vin,  
                    status, 
                    stage_name, 
                    timestamp, 
                    timestamp_dms, 
                    timestamp_hex,
                    amount
                    FROM DW_Servicios
                    WHERE timestamp_dms >= @lastExecution						
                    ORDER BY timestamp_hex asc";
            }

            public static class Total
            {
                public const string Select = @"sp_get_services";
            }

            public static class Quiter
            {
                public const string Select = @"SELECT 
                    idAgency,
                    order_dms, 
                    service_date,
                    service_type, 
                    service_to_performe AS service_to_perform,
                    km AS kms,
                    CAST(NULL AS NVARCHAR(64)) AS ndClientDMS,
                    CAST(NULL AS NVARCHAR(256)) AS nmVendedor,
                    vin,  
                    status, 
                    stage_name, 
                    timestamp, 
                    timestamp_dms, 
                    timestamp_hex,
                    amount
                    FROM view_dwh_services
                    WHERE timestamp_dms >= @lastExecution						
                    ORDER BY timestamp_hex asc";
            }
        }

        public static class ServicesByVin
        {
            public static class Incadea
            {
                public const string Select = @"SELECT * FROM view_dwh_services_by_vin WHERE timestamp_dms >= @start_date ORDER BY timestamp_dms asc";
            }

            public static class Total
            {
                public const string Select = @"sp_get_services_by_vin";
            }

            public static class Quiter
            {
                public const string Select = @"SELECT * FROM view_dwh_services_by_vin WHERE timestamp_dms >= @start_date ORDER BY timestamp_dms asc";
            }

            
        }

        public static class Spares
        {
            public static class Incadea
            {
                public const string Select = @"SELECT 
                ndPlant As idAgency,
                sku,
                bussines_name,
                agency,
                warehouse_number,
                part_number,
                description,
                desctiption_ext,
                cost,
                location,
                family,
                available,
                reserved,
                public_cost,
                first_purchase,
                last_purchase,
                last_sale,
                timestamp_dms,
                timestamp_hex,
                timestamp
                FROM DW_Refacciones
                WHERE timestamp_dms >= @lastExecution'						
                ORDER BY timestamp_hex asc";
            }

            public static class Total
            {
                public const string Select = "sp_get_spare_parts_inventory";
            }

            public static class Quitter
            {
                
            }
        }

        public static class CustomersVehicle
        {
            public static class Total
            {
                public const string Select = "SELECT * FROM view_get_customer_vehicle WHERE timestamp_dms >= @lastExecution ORDER BY timestamp_hex ASC";
            }

            public static class Incadea
            {
                public const string Select = @"SELECT 
                idAgency,
                ndPlant,
                ndClientDMS,
                version,
                customerName,
                vin,
                brand,
                model,
                year,
                plates,
                external_color,
                internal_color,
                insurance_expiration_date,
                insurance_number,
                insurance_company,
                timestamp_dms,
                timestamp_hex,
                timestamp,
                timestamp_insurance_info
                FROM DW_Vehiculos_Clientes
                WHERE timestamp_dms >= @lastExecution
                ORDER BY timestamp_hex asc";
            }

            public static class Quiter
            {
                public const string Select = "SELECT * FROM view_get_customer_vehicle WHERE timestamp_dms >= @lastExecution ORDER BY timestamp_hex ASC";
            }
        }

        public static class LastCustomerSale
        {
            public static class Total
            {
                public const string Select = "sp_get_last_customer_seller";
            }

            public static class Incadea
            {
                public const string Select = @"select 
	                10018 as idAgency, 
	                order_dms as order_dms, 
	                ndClientDms as ndClientDms,
	                bussines_name as customerName,
	                ndSeller as ndConsultant,
	                seller_Name as consultantName,
	                seller_Mail as consultantMail,
	                last_sale as order_timestamp
                from [MEX-VAN-incadea].[dbo].[view_dwh_last_customer_seller]
                WHERE timestamp_dms >= @lastExecution and ndClientDms is not null and order_dms is not null ORDER BY timestamp_dms ASC";
            }

            public static class Quitter
            {
               
            }
        }

        public static class Vehicle_orders
        {
            public static class Total
            {
                public const string Select = "sp_get_vehicle_orders";
            }

            public static class Incadea
            {
                // TRY_CONVERT prevents "varchar to datetime out-of-range" errors when source has invalid date strings
                public const string Select = "SELECT * FROM view_dwh_vehicle_orders WHERE TRY_CONVERT(datetime, timestamp_dms) >= @lastExecution AND TRY_CONVERT(datetime, timestamp_dms) IS NOT NULL";
            }

            public static class Quiter
            {
                // TRY_CONVERT prevents "varchar to datetime out-of-range" errors when source has invalid date strings
                public const string Select = "SELECT * FROM view_dwh_vehicle_orders WHERE TRY_CONVERT(datetime, timestamp_dms) >= @lastExecution AND TRY_CONVERT(datetime, timestamp_dms) IS NOT NULL";
            }
        }

        public static class Commisions
        {
            public static class Total
            {
                public const string Select = "sp_get_comissions";
            }

            public static class Incadea
            {

            }

            public static class Quitter
            {
            }
        }

        public static class Lead
        {
            public static class Total
            {
                public const string Select = "SELECT * FROM view_get_dwh_lead";
            }

            public static class Incadea
            {
                public const string Select = @"
                SELECT 
                    lead.No_ as LeadNo,
                    lead.[OEM Lead ID] as OemLeadId,
                    contact.Name as FullName,
                    contact.[Home Phone No_] as Phone,
	                COALESCE(NULLIF(LTRIM(RTRIM(contact.[Home E-Mail])), ''), NULLIF(LTRIM(RTRIM(contact.[E-Mail])), '')) AS Email,
	                contact.[First Name] as FirstName,
	                contact.[Last Name] as LastName,
	                contact.[Address Salutation Code] as Salutation,
                    lead.[Lead Description 2] as Campaign,
                    lead.[Lead Description] as description,

                    -- Parsed parts
                    LTRIM(RTRIM(parsed.Oem))    AS Oem,
                    LTRIM(RTRIM(parsed.[Type])) AS [Type],
                    LTRIM(RTRIM(parsed.DueDate))AS DueDate,

                    -- Model code / name from the last segment separated by '|'
                    LTRIM(RTRIM(model.Brand)) AS Brand,
                    LTRIM(RTRIM(model.Model)) AS Model,

                    contact.[Creation Date] as timestamp,
                    lead.timestamp as [timestamp_hex]
                FROM [Vanauto W SA de CV$Lead Buffer] AS lead
                LEFT JOIN [Vanauto W SA de CV$Contact Buffer] AS contact
                    ON lead.[External Contact ID] = contact.[External Contact No_]
                CROSS APPLY (
                    -- build XML by replacing the "" - "" separator with XML nodes
                    SELECT CAST('<r>' + 
                                REPLACE(ISNULL(lead.[Lead Description], ''), ' - ', '</r><r>') 
                                + '</r>' AS XML) AS x
                ) AS xmlDesc
                CROSS APPLY (
                    -- extract the common positions and the last part (last segment may contain the '|' model info)
                    SELECT
                        xmlDesc.x.value('(/r[1])[1]', 'varchar(400)')  AS Oem,
                        xmlDesc.x.value('(/r[2])[1]', 'varchar(400)')  AS [Type],
                        xmlDesc.x.value('(/r[3])[1]', 'varchar(400)')  AS DueDate,
                        -- the last segment (whatever the number of parts is)
                        xmlDesc.x.value('(/r[count(/r)])[1]', 'varchar(400)') AS LastPart
                ) AS parsed
                CROSS APPLY (
                    -- split the LastPart by '|' into ModelCode and ModelName (safe if '|' missing)
                    SELECT
                        CASE WHEN CHARINDEX('|', parsed.LastPart) > 0 
                                THEN LEFT(parsed.LastPart, CHARINDEX('|', parsed.LastPart) - 1)
                                ELSE NULL END AS Brand,
                        CASE WHEN CHARINDEX('|', parsed.LastPart) > 0 
                                THEN LTRIM(SUBSTRING(parsed.LastPart, CHARINDEX('|', parsed.LastPart) + 1, 400))
                                ELSE NULL END AS Model
                ) AS model
                WHERE contact.Name IS NOT NULL
                    AND contact.[Home Phone No_] IS NOT NULL
                    AND contact.[Home E-Mail] IS NOT NULL
                    AND contact.[Creation Date] IS NOT NULL
					AND DueDate IS NOT NULL 
					AND Model IS NOT NULL
					AND contact.[Creation Date] >= CAST(GETDATE() AS date)
                    AND contact.[Creation Date] <  DATEADD(day, 1, CAST(GETDATE() AS date))
                ORDER BY lead.No_ ASC;
                ";
    //            public const string Select = @"
				//	 SELECT top 10
				//	 lead.No_ , 
				//	 lead.[OEM Lead ID] , 
				//	 contact.Name , 
				//	 contact.[Home Phone No_] , 
				//	 contact.[Home E-Mail] , 
				//	 lead.[Lead Description 2] ,
				//	 lead.[Lead Description] ,
				//	 contact.[Creation Date] ,
				//	 lead.timestamp as [timestamp_hex]
				//	 FROM [Vanauto W SA de CV$Lead Buffer] AS lead
				//	 LEFT JOIN [Vanauto W SA de CV$Contact Buffer] AS contact
				//	 ON lead.[External Contact ID] = contact.[External Contact No_]
				//	 ORDER BY lead.No_ ASC
				//";
            }

            public static class Quiter
            {
                public const string Select = "SELECT * FROM view_get_dwh_lead WHERE timestamp_dms >= @lastExecution";
            }
        }
    }
    // Aquí se pueden agregar más extractores en el futuro
    // Por ejemplo:
    // public static class Sales { ... }
    // public static class Service { ... }
    // public static class Customers { ... }

}

<?xml version="1.0" encoding="utf-8" ?>
<configuration>
  <connectionStrings>
    
    <!-- TD Connections -->
    <add name="HondaConnection" connectionString="Data Source=192.168.190.123;Initial Catalog=SQLHONDA;User ID=sa;Password=TotalDealer!"/>    
    <add name="KiaConnection" connectionString="Data Source=192.168.190.116;Initial Catalog=KIASQL;User ID=customerss;Password=V4ndu4rd1a2022" />
    <add name="AudiConnection" connectionString="Data Source=192.168.190.119;Initial Catalog=AUDISQL;User ID=customerss;Password=00@DealerSolutions" />
    <add name="MotonovaConnection" connectionString="Data Source=192.168.190.122;Initial Catalog=MOTOSQL;User ID=Tdealer;Password=Total10" />
    <add name="OmodaConnection" connectionString="Data Source=VGDSRV-TDOMODA1\TDOMODA;Initial Catalog=OMODASQL;User ID=customerss;Password=BDomvgd.24!" />
    <add name="GeelyConnection" connectionString="Data Source=192.168.190.170;Initial Catalog=GEELYG;User ID=NexxusQ;Password=xE?yJ}R7&lt;" />
    <add name="ChireyConnection" connectionString="Data Source=192.168.190.145;Initial Catalog=SQLCHIREY;User ID=NexxusQ;Password=xE?yJ}R7&lt;" />
    
     <!-- Quiter Connection -->
    <add name="RenaultConnection" connectionString="Data Source=192.168.190.72;Initial Catalog=quiterqbi;User ID=NexxusQ;Password=xE?yJ}R7&lt;" />
    
    <!-- BMW Connection -->
    <add name="BMWConnection" connectionString="Data Source=192.168.21.19;Initial Catalog=MEX-VAN-incadea;User ID=customerss;Password=00@DealerSolutions" />
    
     <!-- DWH Connections -->
    <add name="DWHProductionConnection" connectionString="server=192.168.190.140;uid=vgd_testing;pwd=00@DealerSolutions;database=vgd_dwh_prod" />
    <add name="DWHDevelopConnection" connectionString="server=192.168.190.140;uid=vgd_testing;pwd=00@DealerSolutions;database=vgd_dwh_test" />
  </connectionStrings>
  <appSettings>
    <add key="sqlCommandTimeout" value="300"/> <!-- SQL command timeout in seconds (default 30, use 300 for slow queries) -->
    <add key="custumerExecutionTime" value="3600000"/> <!-- cada hora -->
    <add key="inventoryExecutionTime" value="3600000"/> <!-- cada hora 3600000 -->
    <add key="contactsExecutionTime" value="3600000"/>
    <add key="invoicesExecutionTime" value="60000"/>
    <add key="servicesExecutionTime" value="300000"/>
    <add key="spearsExecutionTime" value="3600000"/>
    <add key="ordersExecutionTime" value="5000"/>     <!-- cada hora 30000 -->
    <!-- cada hora -->
    <!-- production / develop -->
    <add key="Environment" value="production"/>
  </appSettings>
</configuration>