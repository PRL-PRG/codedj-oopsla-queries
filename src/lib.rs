use std::path::Path;

use djanco::*;
use djanco::data::*;
use djanco::log::*;
use djanco::csv::*;
use djanco::objects::*;
use djanco_ext::*;

#[djanco(April, 2021, subsets(Generic))]
pub fn my_query(database: &Database, _log: &Log, output: &Path) -> Result<(), std::io::Error>  {
    database.projects()        
        .filter_by(AtLeast(Count(FromEachIf(project::Paths, Equal(path::Language, Language::Java))), 1))
        .filter_by(AtLeast(project::Stars, 50))
        .into_csv_in_dir(output, "s1_a_study_of_potential_code_borrowing.csv")
}