import pandas as pd
import json
from datetime import datetime
from typing import Dict, List, Tuple
from enum import Enum
import re
from getpass import getpass
from sqlalchemy import create_engine

# Clase para clasificar resultados de validación
class ValidationResultType(Enum):
    NOTIFICATION = "NOTIFICACION"
    REJECTION = "RECHAZO"

# Clase para almacenar resultados de validación
class ValidationResult:
    def __init__(self, result_type: ValidationResultType, code: str, description: str, field: str = "", source: str = "RIPS"):
        self.result_type = result_type
        self.code = code
        self.description = description
        self.field = field
        self.source = source

    def to_dict(self) -> Dict:
        return {
            "Clase": self.result_type.value,
            "Codigo": self.code,
            "Descripcion": self.description,
            "Observaciones": self.field,
            "Fuente": self.source
        }

# Clase base para validadores de archivos RIPS
class RIPSValidator:
    def __init__(self, engine):
        self.engine = engine
        self.results: List[ValidationResult] = []
        self.catalogs = {
            "CUPS": ["890501", "890601"],
            "CIE10": ["E119", "J459"],
            "ATC": ["A10BA02"],
            "TIPO_DOCUMENTO": ["CC", "TI", "CE", "PA"],
            "TIPO_NOTA": ["SF", "RS"],
            "CONCEPTO_RECAUDO": ["01", "02", "03"],
            "VIA_INGRESO": ["01", "02", "03"],
            "CAUSA_EXTERNA": ["01", "02", "15"],
            "TIPO_OS": ["01", "02", "03"]
        }

    def validate_string(self, value: str, field: str, max_length: int, required: bool = True, regex: str = None) -> bool:
        if required and (value is None or str(value).strip() == ""):
            self.results.append(ValidationResult(
                ValidationResultType.REJECTION,
                f"ERR_{field}_001",
                f"El campo {field} es obligatorio",
                field
            ))
            return False
        if value and len(str(value)) > max_length:
            self.results.append(ValidationResult(
                ValidationResultType.REJECTION,
                f"ERR_{field}_002",
                f"El campo {field} excede el tamaño máximo de {max_length}",
                field
            ))
            return False
        if value and regex and not re.match(regex, str(value)):
            self.results.append(ValidationResult(
                ValidationResultType.REJECTION,
                f"ERR_{field}_003",
                f"El campo {field} no cumple el formato esperado",
                field
            ))
            return False
        return True

    def validate_code(self, value: str, field: str, catalog: str) -> bool:
        if value and value not in self.catalogs.get(catalog, []):
            self.results.append(ValidationResult(
                ValidationResultType.NOTIFICATION,
                f"WARN_{field}_001",
                f"El código {value} en {field} no está en el catálogo {catalog}",
                field
            ))
            return False
        return True

    def validate_date(self, value: str, field: str, required: bool = True) -> bool:
        if required and (value is None or str(value).strip() == ""):
            self.results.append(ValidationResult(
                ValidationResultType.REJECTION,
                f"ERR_{field}_001",
                f"El campo {field} es obligatorio",
                field
            ))
            return False
        if value:
            try:
                datetime.strptime(str(value), "%Y-%m-%d %H:%M:%S")
            except ValueError:
                self.results.append(ValidationResult(
                    ValidationResultType.REJECTION,
                    f"ERR_{field}_004",
                    f"El campo {field} no tiene un formato de fecha válido",
                    field
                ))
                return False
        return True

    def validate_number(self, value: float, field: str, min_value: float = None, max_value: float = None) -> bool:
        if value is None or pd.isna(value):
            self.results.append(ValidationResult(
                ValidationResultType.REJECTION,
                f"ERR_{field}_001",
                f"El campo {field} es obligatorio",
                field
            ))
            return False
        if min_value is not None and value < min_value:
            self.results.append(ValidationResult(
                ValidationResultType.REJECTION,
                f"ERR_{field}_005",
                f"El campo {field} debe ser mayor o igual a {min_value}",
                field
            ))
            return False
        if max_value is not None and value > max_value:
            self.results.append(ValidationResult(
                ValidationResultType.REJECTION,
                f"ERR_{field}_006",
                f"El campo {field} debe ser menor o igual a {max_value}",
                field
            ))
            return False
        return True

# Validador para Transacciones (AF)
class TransaccionesValidator(RIPSValidator):
    def validate(self, consecutivo: str) -> Tuple[bool, List[Dict]]:
        query = """
        SELECT codPrestador, consecutivo, numDocumentoIdObligado, numFactura, tipoNota, numNota
        FROM Transacciones
        WHERE consecutivo = ?
        """
        try:
            df = pd.read_sql(query, self.engine, params=[(consecutivo,)])
            if df.empty:
                self.results.append(ValidationResult(
                    ValidationResultType.REJECTION,
                    "ERR_AF_001",
                    f"No se encontró transacción con consecutivo {consecutivo}"
                ))
                return False, []

            record = df.iloc[0]
            valid = True

            valid &= self.validate_string(record["codPrestador"], "codPrestador", 12)
            valid &= self.validate_string(record["numDocumentoIdObligado"], "numDocumentoIdObligado", 20)
            valid &= self.validate_string(record["numFactura"], "numFactura", 20, required=False)
            valid &= self.validate_string(record["tipoNota"], "tipoNota", 2, required=False)
            valid &= self.validate_string(record["numNota"], "numNota", 20, required=False)

            if record["tipoNota"]:
                valid &= self.validate_code(record["tipoNota"], "tipoNota", "TIPO_NOTA")
                if record["tipoNota"] == "RS" and pd.isna(record["numFactura"]):
                    self.results.append(ValidationResult(
                        ValidationResultType.NOTIFICATION,
                        "INFO_AF_001",
                        "RIPS sin factura permitido con tipoNota RS",
                        "tipoNota"
                    ))

            return valid, df.to_dict(orient="records")
        except Exception as e:
            self.results.append(ValidationResult(
                ValidationResultType.REJECTION,
                "ERR_AF_002",
                f"Error en la consulta: {str(e)}"
            ))
            return False, []

# Validador para Usuarios (US)
class UsuariosValidator(RIPSValidator):
    def validate(self, consecutivo: str) -> Tuple[bool, List[Dict]]:
        query = """
        SELECT tipoDocumentoIdentificacion, numDocumentoIdentificacion, codPaisOrigen
        FROM Usuarios
        WHERE numDocumentoIdentificacion = ?
        """
        try:
            df = pd.read_sql(query, self.engine, params=[(consecutivo,)])
            if df.empty:
                self.results.append(ValidationResult(
                    ValidationResultType.REJECTION,
                    "ERR_US_001",
                    f"No se encontró usuario con ID {consecutivo}"
                ))
                return False, []

            record = df.iloc[0]
            valid = True

            valid &= self.validate_string(record["tipoDocumentoIdentificacion"], "tipoDocumentoIdentificacion", 2)
            valid &= self.validate_string(record["numDocumentoIdentificacion"], "numDocumentoIdentificacion", 20)
            valid &= self.validate_string(record["codPaisOrigen"], "codPaisOrigen", 3)

            valid &= self.validate_code(record["tipoDocumentoIdentificacion"], "tipoDocumentoIdentificacion", "TIPO_DOCUMENTO")

            return valid, df.to_dict(orient="records")
        except Exception as e:
            self.results.append(ValidationResult(
                ValidationResultType.REJECTION,
                "ERR_US_002",
                f"Error en la consulta: {str(e)}"
            ))
            return False, []

# Validador para Consultas (AC)
class ConsultasValidator(RIPSValidator):
    def validate(self, consecutivo: str) -> Tuple[bool, List[Dict]]:
        query = """
        SELECT codPrestador, consecutivo, numDocumentoIdentificacion, fechaConsulta,
               codConsulta, codDiagnosticoPrincipal, conceptoRecaudo
        FROM Consultas
        WHERE consecutivo = ?
        """
        try:
            df = pd.read_sql(query, self.engine, params=[(consecutivo,)])
            if df.empty:
                self.results.append(ValidationResult(
                    ValidationResultType.REJECTION,
                    "ERR_AC_001",
                    f"No se encontró consulta con consecutivo {consecutivo}"
                ))
                return False, []

            record = df.iloc[0]
            valid = True

            valid &= self.validate_string(record["codPrestador"], "codPrestador", 12)
            valid &= self.validate_string(record["numDocumentoIdentificacion"], "numDocumentoIdentificacion", 20)
            valid &= self.validate_date(record["fechaConsulta"], "fechaConsulta")
            valid &= self.validate_string(record["codConsulta"], "codConsulta", 6)
            valid &= self.validate_string(record["codDiagnosticoPrincipal"], "codDiagnosticoPrincipal", 25)
            valid &= self.validate_string(record["conceptoRecaudo"], "conceptoRecaudo", 2, required=False)

            if record["codConsulta"]:
                valid &= self.validate_code(record["codConsulta"], "codConsulta", "CUPS")
                if record["codConsulta"] in ["890501", "890601"] and record["conceptoRecaudo"] == "01":
                    self.results.append(ValidationResult(
                        ValidationResultType.NOTIFICATION,
                        "INFO_AC_001",
                        "Copago permitido para CUPS 8905/8906",
                        "conceptoRecaudo"
                    ))
                elif record["conceptoRecaudo"] == "01" and record["codConsulta"] not in ["890501", "890601"]:
                    self.results.append(ValidationResult(
                        ValidationResultType.REJECTION,
                        "ERR_AC_002",
                        "Copago no permitido para este CUPS",
                        "conceptoRecaudo"
                    ))
                    valid = False
            if record["codDiagnosticoPrincipal"]:
                valid &= self.validate_code(record["codDiagnosticoPrincipal"], "codDiagnosticoPrincipal", "CIE10")

            query_user = "SELECT COUNT(*) FROM Usuarios WHERE numDocumentoIdentificacion = ?"
            user_count = pd.read_sql(query_user, self.engine, params=[(record["numDocumentoIdentificacion"],)]).iloc[0, 0]
            if user_count == 0:
                self.results.append(ValidationResult(
                    ValidationResultType.REJECTION,
                    "ERR_AC_003",
                    f"El usuario {record['numDocumentoIdentificacion']} no existe en Usuarios",
                    "numDocumentoIdentificacion"
                ))
                valid = False

            return valid, df.to_dict(orient="records")
        except Exception as e:
            self.results.append(ValidationResult(
                ValidationResultType.REJECTION,
                "ERR_AC_004",
                f"Error en la consulta: {str(e)}"
            ))
            return False, []

# Validador para Procedimientos (AP)
class ProcedimientosValidator(RIPSValidator):
    def validate(self, consecutivo: str) -> Tuple[bool, List[Dict]]:
        query = """
        SELECT codPrestador, consecutivo, numDocumentoIdentificacion, fechaInicioAtencion,
               numAutorizacion, codProcedimiento, viaIngreso
        FROM Procedimientos
        WHERE consecutivo = ?
        """
        try:
            df = pd.read_sql(query, self.engine, params=[(consecutivo,)])
            if df.empty:
                self.results.append(ValidationResult(
                    ValidationResultType.REJECTION,
                    "ERR_AP_001",
                    f"No se encontró procedimiento con consecutivo {consecutivo}"
                ))
                return False, []

            record = df.iloc[0]
            valid = True

            valid &= self.validate_string(record["codPrestador"], "codPrestador", 12)
            valid &= self.validate_string(record["numDocumentoIdentificacion"], "numDocumentoIdentificacion", 20)
            valid &= self.validate_date(record["fechaInicioAtencion"], "fechaInicioAtencion")
            valid &= self.validate_string(record["numAutorizacion"], "numAutorizacion", 30, required=False)
            valid &= self.validate_string(record["codProcedimiento"], "codProcedimiento", 6)
            valid &= self.validate_string(record["viaIngreso"], "viaIngreso", 2)

            valid &= self.validate_code(record["codProcedimiento"], "codProcedimiento", "CUPS")
            valid &= self.validate_code(record["viaIngreso"], "viaIngreso", "VIA_INGRESO")

            if pd.isna(record["numAutorizacion"]):
                self.results.append(ValidationResult(
                    ValidationResultType.NOTIFICATION,
                    "INFO_AP_001",
                    "Autorización no requerida para este procedimiento",
                    "numAutorizacion"
                ))

            query_user = "SELECT COUNT(*) FROM Usuarios WHERE numDocumentoIdentificacion = ?"
            user_count = pd.read_sql(query_user, self.engine, params=[(record["numDocumentoIdentificacion"],)]).iloc[0, 0]
            if user_count == 0:
                self.results.append(ValidationResult(
                    ValidationResultType.REJECTION,
                    "ERR_AP_002",
                    f"El usuario {record['numDocumentoIdentificacion']} no existe en Usuarios",
                    "numDocumentoIdentificacion"
                ))
                valid = False

            return valid, df.to_dict(orient="records")
        except Exception as e:
            self.results.append(ValidationResult(
                ValidationResultType.REJECTION,
                "ERR_AP_003",
                f"Error en la consulta: {str(e)}"
            ))
            return False, []

# Validador para Urgencias (AU)
class UrgenciasValidator(RIPSValidator):
    def validate(self, consecutivo: str) -> Tuple[bool, List[Dict]]:
        query = """
        SELECT codPrestador, consecutivo, numDocumentoIdentificacion, fechaInicioAtencion,
               causaExterna, codDiagnosticoPrincipal, condicionDestinoUsuarioEgreso
        FROM Urgencias
        WHERE consecutivo = ?
        """
        try:
            df = pd.read_sql(query, self.engine, params=[(consecutivo,)])
            if df.empty:
                self.results.append(ValidationResult(
                    ValidationResultType.REJECTION,
                    "ERR_AU_001",
                    f"No se encontró urgencia con consecutivo {consecutivo}"
                ))
                return False, []

            record = df.iloc[0]
            valid = True

            valid &= self.validate_string(record["codPrestador"], "codPrestador", 12)
            valid &= self.validate_string(record["numDocumentoIdentificacion"], "numDocumentoIdentificacion", 20)
            valid &= self.validate_date(record["fechaInicioAtencion"], "fechaInicioAtencion")
            valid &= self.validate_string(record["causaExterna"], "causaExterna", 2)
            valid &= self.validate_string(record["codDiagnosticoPrincipal"], "codDiagnosticoPrincipal", 25)
            valid &= self.validate_string(record["condicionDestinoUsuarioEgreso"], "condicionDestinoUsuarioEgreso", 2)

            valid &= self.validate_code(record["causaExterna"], "causaExterna", "CAUSA_EXTERNA")
            valid &= self.validate_code(record["codDiagnosticoPrincipal"], "codDiagnosticoPrincipal", "CIE10")

            query_user = "SELECT COUNT(*) FROM Usuarios WHERE numDocumentoIdentificacion = ?"
            user_count = pd.read_sql(query_user, self.engine, params=[(record["numDocumentoIdentificacion"],)]).iloc[0, 0]
            if user_count == 0:
                self.results.append(ValidationResult(
                    ValidationResultType.REJECTION,
                    "ERR_AU_002",
                    f"El usuario {record['numDocumentoIdentificacion']} no existe en Usuarios",
                    "numDocumentoIdentificacion"
                ))
                valid = False

            return valid, df.to_dict(orient="records")
        except Exception as e:
            self.results.append(ValidationResult(
                ValidationResultType.REJECTION,
                "ERR_AU_003",
                f"Error en la consulta: {str(e)}"
            ))
            return False, []

# Validador para Hospitalización (AH)
class HospitalizacionValidator(RIPSValidator):
    def validate(self, consecutivo: str) -> Tuple[bool, List[Dict]]:
        query = """
        SELECT codPrestador, consecutivo, numDocumentoIdentificacion, fechaIngreso,
               codDiagnosticoPrincipal, fechaEgreso, codDiagnosticoCausaMuerte
        FROM Hospitalización
        WHERE consecutivo = ?
        """
        try:
            df = pd.read_sql(query, self.engine, params=[(consecutivo,)])
            if df.empty:
                self.results.append(ValidationResult(
                    ValidationResultType.REJECTION,
                    "ERR_AH_001",
                    f"No se encontró hospitalización con consecutivo {consecutivo}"
                ))
                return False, []

            record = df.iloc[0]
            valid = True

            valid &= self.validate_string(record["codPrestador"], "codPrestador", 12)
            valid &= self.validate_string(record["numDocumentoIdentificacion"], "numDocumentoIdentificacion", 20)
            valid &= self.validate_date(record["fechaIngreso"], "fechaIngreso")
            valid &= self.validate_string(record["codDiagnosticoPrincipal"], "codDiagnosticoPrincipal", 25)
            valid &= self.validate_date(record["fechaEgreso"], "fechaEgreso")
            valid &= self.validate_string(record["codDiagnosticoCausaMuerte"], "codDiagnosticoCausaMuerte", 25, required=False)

            valid &= self.validate_code(record["codDiagnosticoPrincipal"], "codDiagnosticoPrincipal", "CIE10")
            if record["codDiagnosticoCausaMuerte"]:
                valid &= self.validate_code(record["codDiagnosticoCausaMuerte"], "codDiagnosticoCausaMuerte", "CIE10")

            if record["fechaIngreso"] and record["fechaEgreso"]:
                fecha_ingreso = datetime.strptime(str(record["fechaIngreso"]), "%Y-%m-%d %H:%M:%S")
                fecha_egreso = datetime.strptime(str(record["fechaEgreso"]), "%Y-%m-%d %H:%M:%S")
                if fecha_egreso < fecha_ingreso:
                    self.results.append(ValidationResult(
                        ValidationResultType.REJECTION,
                        "ERR_AH_002",
                        "La fecha de egreso no puede ser anterior a la fecha de ingreso",
                        "fechaEgreso"
                    ))
                    valid = False

            query_user = "SELECT COUNT(*) FROM Usuarios WHERE numDocumentoIdentificacion = ?"
            user_count = pd.read_sql(query_user, self.engine, params=[(record["numDocumentoIdentificacion"],)]).iloc[0, 0]
            if user_count == 0:
                self.results.append(ValidationResult(
                    ValidationResultType.REJECTION,
                    "ERR_AH_003",
                    f"El usuario {record['numDocumentoIdentificacion']} no existe en Usuarios",
                    "numDocumentoIdentificacion"
                ))
                valid = False

            return valid, df.to_dict(orient="records")
        except Exception as e:
            self.results.append(ValidationResult(
                ValidationResultType.REJECTION,
                "ERR_AH_004",
                f"Error en la consulta: {str(e)}"
            ))
            return False, []

# Validador para Recién Nacido (AN)
class RecienNacidoValidator(RIPSValidator):
    def validate(self, consecutivo: str) -> Tuple[bool, List[Dict]]:
        query = """
        SELECT codPrestador, consecutivo, numDocumentoIdentificacion, fechaNacimiento,
               sexoRecienNacido, pesoRecienNacido
        FROM Recién_Nacido
        WHERE consecutivo = ?
        """
        try:
            df = pd.read_sql(query, self.engine, params=[(consecutivo,)])
            if df.empty:
                self.results.append(ValidationResult(
                    ValidationResultType.REJECTION,
                    "ERR_AN_001",
                    f"No se encontró recién nacido con consecutivo {consecutivo}"
                ))
                return False, []

            record = df.iloc[0]
            valid = True

            valid &= self.validate_string(record["codPrestador"], "codPrestador", 12)
            valid &= self.validate_string(record["numDocumentoIdentificacion"], "numDocumentoIdentificacion", 20)
            valid &= self.validate_date(record["fechaNacimiento"], "fechaNacimiento")
            valid &= self.validate_string(record["sexoRecienNacido"], "sexoRecienNacido", 1)
            valid &= self.validate_number(record["pesoRecienNacido"], "pesoRecienNacido", min_value=0.1)

            if record["sexoRecienNacido"] not in ["M", "F"]:
                self.results.append(ValidationResult(
                    ValidationResultType.REJECTION,
                    "ERR_AN_002",
                    "El sexo del recién nacido debe ser M o F",
                    "sexoRecienNacido"
                ))
                valid = False

            query_user = "SELECT COUNT(*) FROM Usuarios WHERE numDocumentoIdentificacion = ?"
            user_count = pd.read_sql(query_user, self.engine, params=[(record["numDocumentoIdentificacion"],)]).iloc[0, 0]
            if user_count == 0:
                self.results.append(ValidationResult(
                    ValidationResultType.REJECTION,
                    "ERR_AN_003",
                    f"El usuario {record['numDocumentoIdentificacion']} no existe en Usuarios",
                    "numDocumentoIdentificacion"
                ))
                valid = False

            return valid, df.to_dict(orient="records")
        except Exception as e:
            self.results.append(ValidationResult(
                ValidationResultType.REJECTION,
                "ERR_AN_004",
                f"Error en la consulta: {str(e)}"
            ))
            return False, []

# Validador para Medicamentos (AM)
class MedicamentosValidator(RIPSValidator):
    def validate(self, consecutivo: str) -> Tuple[bool, List[Dict]]:
        query = """
        SELECT codPrestador, consecutivo, numDocumentoIdentificacion, codTecnologiaSalud,
               nomTecnologiaSalud, concentracionMedicamento, formaFarmaceutica
        FROM Medicamentos
        WHERE consecutivo = ?
        """
        try:
            df = pd.read_sql(query, self.engine, params=[(consecutivo,)])
            if df.empty:
                self.results.append(ValidationResult(
                    ValidationResultType.REJECTION,
                    "ERR_AM_001",
                    f"No se encontró medicamento con consecutivo {consecutivo}"
                ))
                return False, []

            record = df.iloc[0]
            valid = True

            valid &= self.validate_string(record["codPrestador"], "codPrestador", 12)
            valid &= self.validate_string(record["numDocumentoIdentificacion"], "numDocumentoIdentificacion", 20)
            valid &= self.validate_string(record["codTecnologiaSalud"], "codTecnologiaSalud", 7)
            valid &= self.validate_string(record["nomTecnologiaSalud"], "nomTecnologiaSalud", 150)
            valid &= self.validate_string(record["concentracionMedicamento"], "concentracionMedicamento", 3, required=False)
            valid &= self.validate_string(record["formaFarmaceutica"], "formaFarmaceutica", 20, required=False)

            if record["codTecnologiaSalud"]:
                valid &= self.validate_code(record["codTecnologiaSalud"], "codTecnologiaSalud", "ATC")

            if record["nomTecnologiaSalud"] and "magistral" in str(record["nomTecnologiaSalud"]).lower():
                self.results.append(ValidationResult(
                    ValidationResultType.NOTIFICATION,
                    "INFO_AM_001",
                    "Preparación magistral detectada, se permite un solo principio activo",
                    "nomTecnologiaSalud"
                ))
                if record["concentracionMedicamento"] and "." in str(record["concentracionMedicamento"]):
                    self.results.append(ValidationResult(
                        ValidationResultType.NOTIFICATION,
                        "WARN_AM_002",
                        "Decimales en concentracionMedicamento no soportados aún (cambio pendiente)",
                        "concentracionMedicamento"
                    ))

            query_user = "SELECT COUNT(*) FROM Usuarios WHERE numDocumentoIdentificacion = ?"
            user_count = pd.read_sql(query_user, self.engine, params=[(record["numDocumentoIdentificacion"],)]).iloc[0, 0]
            if user_count == 0:
                self.results.append(ValidationResult(
                    ValidationResultType.REJECTION,
                    "ERR_AM_003",
                    f"El usuario {record['numDocumentoIdentificacion']} no existe en Usuarios",
                    "numDocumentoIdentificacion"
                ))
                valid = False

            return valid, df.to_dict(orient="records")
        except Exception as e:
            self.results.append(ValidationResult(
                ValidationResultType.REJECTION,
                "ERR_AM_004",
                f"Error en la consulta: {str(e)}"
            ))
            return False, []

# Validador para Otros Servicios (AT)
class OtrosServiciosValidator(RIPSValidator):
    def validate(self, consecutivo: str) -> Tuple[bool, List[Dict]]:
        query = """
        SELECT codPrestador, consecutivo, numDocumentoIdentificacion, tipoOS,
               codTecnologiaSalud, cantidadOS
        FROM Otros_Servicios
        WHERE consecutivo = ?
        """
        try:
            df = pd.read_sql(query, self.engine, params=[(consecutivo,)])
            if df.empty:
                self.results.append(ValidationResult(
                    ValidationResultType.REJECTION,
                    "ERR_AT_001",
                    f"No se encontró otro servicio con consecutivo {consecutivo}"
                ))
                return False, []

            record = df.iloc[0]
            valid = True

            valid &= self.validate_string(record["codPrestador"], "codPrestador", 12)
            valid &= self.validate_string(record["numDocumentoIdentificacion"], "numDocumentoIdentificacion", 20)
            valid &= self.validate_string(record["tipoOS"], "tipoOS", 2)
            valid &= self.validate_string(record["codTecnologiaSalud"], "codTecnologiaSalud", 7)
            valid &= self.validate_number(record["cantidadOS"], "cantidadOS", min_value=1)

            valid &= self.validate_code(record["tipoOS"], "tipoOS", "TIPO_OS")

            query_user = "SELECT COUNT(*) FROM Usuarios WHERE numDocumentoIdentificacion = ?"
            user_count = pd.read_sql(query_user, self.engine, params=[(record["numDocumentoIdentificacion"],)]).iloc[0, 0]
            if user_count == 0:
                self.results.append(ValidationResult(
                    ValidationResultType.REJECTION,
                    "ERR_AT_002",
                    f"El usuario {record['numDocumentoIdentificacion']} no existe en Usuarios",
                    "numDocumentoIdentificacion"
                ))
                valid = False

            return valid, df.to_dict(orient="records")
        except Exception as e:
            self.results.append(ValidationResult(
                ValidationResultType.REJECTION,
                "ERR_AT_003",
                f"Error en la consulta: {str(e)}"
            ))
            return False, []

# Clase principal para orquestar la validación
class RIPSProcessor:
    def __init__(self, engine):
        self.engine = engine
        self.validators = {
            "AF": TransaccionesValidator(engine),
            "US": UsuariosValidator(engine),
            "AC": ConsultasValidator(engine),
            "AP": ProcedimientosValidator(engine),
            "AU": UrgenciasValidator(engine),
            "AH": HospitalizacionValidator(engine),
            "AN": RecienNacidoValidator(engine),
            "AM": MedicamentosValidator(engine),
            "AT": OtrosServiciosValidator(engine)
        }

    def process(self, file_type: str, consecutivo: str) -> Dict:
        validator = self.validators.get(file_type)
        if not validator:
            return {
                "Valid": False,
                "ValidationResults": [{
                    "Clase": "RECHAZO",
                    "Codigo": "ERR_GEN_001",
                    "Descripcion": f"Tipo de archivo {file_type} no soportado",
                    "Fuente": "RIPS"
                }],
                "JSONGenerated": False
            }

        is_valid, records = validator.validate(consecutivo)
        has_rejections = any(r.result_type == ValidationResultType.REJECTION for r in validator.results)

        response = {
            "Valid": is_valid and not has_rejections,
            "ValidationResults": [r.to_dict() for r in validator.results],
            "JSONGenerated": False
        }

        if response["Valid"]:
            json_file = f"rips_{file_type}_{consecutivo}.json"
            with open(json_file, "w", encoding="utf-8") as f:
                json.dump(records, f, ensure_ascii=False, indent=2)
            response["JSONGenerated"] = True
            response["JSONFile"] = json_file

        return response

# Ejemplo de uso
if __name__ == "__main__":
    # Solicitar la contraseña de forma segura
    password = getpass("Ingrese la contraseña para el usuario sa: ")

    # Crear la cadena de conexión para SQLAlchemy
    conn_str = (
        f"mssql+pyodbc://sa:{password}@localhost:1433/RIPS_AI?"
        f"driver=ODBC+Driver+18+for+SQL+Server&"
        f"Encrypt=no&"
        f"TrustServerCertificate=yes"
    )

    # Crear el motor SQLAlchemy
    engine = create_engine(conn_str)

    processor = RIPSProcessor(engine)

    test_cases = [
        ("AF", "TRANS_001"),
        ("US", "123456"),
        ("AC", "CONS_001"),
        ("AP", "PROC_001"),
        ("AU", "URG_001"),
        ("AH", "HOSP_001"),
        ("AN", "RN_001"),
        ("AM", "MED_001"),
        ("AT", "OS_001")
    ]

    for file_type, consecutivo in test_cases:
        print(f"\nValidando {file_type} con consecutivo {consecutivo}:")
        result = processor.process(file_type, consecutivo)
        print(json.dumps(result, indent=2, ensure_ascii=False))