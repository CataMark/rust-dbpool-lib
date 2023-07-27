use crate::{error::ErrorReport, generics::GenericWrapper};

pub fn naive_xlsx_cast(
    v: &xlsxreader::DataType,
    row: u32,
    col: u32,
) -> Result<GenericWrapper, ErrorReport> {
    let res = match v {
        xlsxreader::DataType::Int(p) => GenericWrapper::from(p),
        xlsxreader::DataType::Float(p) => GenericWrapper::from(p),
        xlsxreader::DataType::String(p) => GenericWrapper::from(p),
        xlsxreader::DataType::Bool(p) => GenericWrapper::from(p),
        xlsxreader::DataType::DateTime(p) => {
            if let Some(t) = v.as_datetime() {
                GenericWrapper::from(t)
            } else {
                GenericWrapper::from(p)
            }
        }
        xlsxreader::DataType::Error(_) => {
            return Err(crate::error::error_xlsx_cell_value(&row, &col))
        }
        xlsxreader::DataType::Empty => GenericWrapper::None,
    };
    Ok(res)
}

pub struct XlsxFormats {
    pub bold: xlsxwriter::Format,
    pub integer: xlsxwriter::Format,
    pub decimal: xlsxwriter::Format,
    pub date: xlsxwriter::Format,
    pub time: xlsxwriter::Format,
    pub timestamp: xlsxwriter::Format,
}

impl XlsxFormats {
    pub fn new() -> Self {
        Self {
            bold: xlsxwriter::Format::new().set_bold(),
            integer: xlsxwriter::Format::new().set_num_format("#,##0"),
            decimal: xlsxwriter::Format::new().set_num_format("#,##0.00"),
            date: xlsxwriter::Format::new().set_num_format("yyyy-mm-dd"),
            time: xlsxwriter::Format::new().set_num_format("HH:mm:ss"),
            timestamp: xlsxwriter::Format::new().set_num_format("yyyy-mm-dd HH:mm:ss"),
        }
    }
}
