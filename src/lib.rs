use std::path::Path;

use djanco::*;
use djanco::data::*;
use djanco::log::*;
use djanco::csv::*;
use djanco::objects::*;
use djanco::fraction::Fraction;
use djanco_ext::*;

#[djanco(April, 2021, subsets(Generic))]
pub fn s1_a_study_of_potential_code_borrowing(database: &Database, _log: &Log, output: &Path) -> Result<(), std::io::Error>  {
    database.projects()        
        .filter_by(AtLeast(Count(FromEachIf(project::Paths, Equal(path::Language, Language::Java))), 1))
        .filter_by(AtLeast(project::Stars, 50))
        .into_csv_in_dir(output, "s1_a_study_of_potential_code_borrowing.csv")
}

#[djanco(April, 2021, subsets(Generic))]
pub fn s2_an_empirical_study_of_method_chaining(database: &Database, _log: &Log, output: &Path) -> Result<(), std::io::Error>  {
    database.projects()
        .filter_by(Equal(project::Language, Language::Java))
        .sort_by(project::Stars)
        .sample(Top(2814))
        .into_csv_in_dir(output, "s2_an_empirical_study_of_method_chaining.csv")
}

#[djanco(April, 2021, subsets(Generic))]
pub fn s3_characterizing_and_identifying_composite_refactorings(database: &Database, _log: &Log, output: &Path) -> Result<(), std::io::Error>  {
    database.projects()        
        .filter_by(Equal(project::Language, Language::Java))
        .filter_by(project::HasIssues)
        .filter_by(AtLeast(Ratio(FromEachIf(project::Paths, Equal(path::Language, Language::Java)), project::Paths), Fraction::new(9,10)))
        .sample(Random(48, Seed(42)))
        .into_csv_in_dir(output, "s3_characterizing_and_identifying_composite_refactorings.csv")
}