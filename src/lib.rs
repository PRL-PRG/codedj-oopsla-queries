use std::path::Path;
use std::collections::HashSet;
use std::path::PathBuf;

use chrono;

use djanco::*;
use djanco::data::*;
use djanco::log::*;
use djanco::csv::*;
use djanco::objects::*;
use djanco::time::Duration;
use djanco::fraction::*;
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

#[djanco(April, 2021, subsets(Generic))]
pub fn s4_developer_driven_code_smell_prioritization(database: &Database, _log: &Log, output: &Path) -> Result<(), std::io::Error>  {
    database.projects()        
        .filter_by(Equal(project::Language, Language::Java))
        .filter_by(Matches(project::URL, regex!("https://github.com/(eclipse|apache)/")))
        .filter_by(AtLeast(project::Age, Duration::from_years(5)))
        .filter_by(MoreThan(Count(project::Users), 20))
        .filter_by(AtLeast(Count(project::Commits), 1000))
        // FIXME this next one shgould probably only refer to the tree or only the master branch
        // Classes more than 500
        .filter_by(MoreThan(Count(FromEachIf(project::Paths, Equal(path::Language, Language::Java))), 500))
        .sample(Random(9, Seed(42)))
        .into_csv_in_dir(output, "s4_developer_driven_code_smell_prioritization.csv")
}

#[djanco(April, 2021, subsets(Generic))]
pub fn s5_did_you_remember_to_test_your_tokens(database: &Database, _log: &Log, output: &Path) -> Result<(), std::io::Error>  {
    let junits_path = format!("{}/{}", output.as_os_str().to_str().unwrap(), "junits.csv");

    if !PathBuf::from(&junits_path).exists() {
        database.snapshots_with_data()
            .filter_by(Contains(snapshot::Contents, "import org.junit"))
            .map_into(snapshot::Id)
            .into_csv(&junits_path).unwrap();
    }

    let junits: HashSet<SnapshotId> = 
        SnapshotId::from_csv(&junits_path).unwrap().into_iter().collect();

    database.projects()        
        .sample(Random(1000, Seed(42)))
        .filter_by(MoreThan(project::Size, 0))
        .filter_by(MoreThan(project::Pushed, timestamp!(January 2021))) // assumed last push threshold
        .filter_by(AnyIn(project::SnapshotIds, junits))
        .into_csv_in_dir(output, "s5_did_you_remember_to_test_your_tokens.csv")
}

#[djanco(April, 2021, subsets(Generic))]
pub fn s6_forking_without_clicking(database: &Database, _log: &Log, output: &Path) -> Result<(), std::io::Error>  {
    database.projects()        
        // TODO project is in Software Heritage Graph Dataset and GHT
        .filter_by(MoreThan(Count(project::Commits), 0)) // assumed        
        .into_csv_in_dir(output, "s6_forking_without_clicking.csv")
}

#[djanco(April, 2021, subsets(Generic))]
pub fn s7_the_state_of_the_ml_universe(database: &Database, _log: &Log, output: &Path) -> Result<(), std::io::Error>  {
    // FIXME first query not doable?
    database.projects()
        // Stage 1
        .filter_by(AtLeast(project::Pushed, timestamp!(January 2019)))
        .filter_by(LessThan(project::Pushed, timestamp!(January 2020)))
        // And remove AI&ML projects
        .sort_by(project::Stars)
        .sample(Random(10000, Seed(42))) 
        // Stage 2
        .filter_by(MoreThan(project::Size, 0)) // non-empty
        .filter_by(Or(AtLeast(project::Stars, 5), AtLeast(project::Forks, 5)))
        // And manually remove student code, etc.
        .into_csv_in_dir(output, "s7_the_state_of_the_ml_universe.csv")
}

#[djanco(April, 2021, subsets(Generic))]
pub fn s8_what_constitutes_software(database: &Database, _log: &Log, output: &Path) -> Result<(), std::io::Error>  {
    // ABAP, ADA not supported
    let hand_picked_languages = vec![
        Language::ASM, Language::C, Language::Cobol, Language::Cpp, Language::CSharp, Language::D, Language::Erlang, Language::Fortran,
        Language::FSharp, Language::Go, Language::Groovy, Language::Java, Language::JavaScript, Language::Kotlin, Language::Lua, Language::ObjectiveC,
        Language::OCaml, Language::Perl, Language::PHP, Language::Python, Language::Ruby, Language::Scala, Language::Swift
    ];

    database.projects()
        .filter_by(Member(project::Language, hand_picked_languages))
        .group_by(project::Language)
        .sample(Top(1020))
        .ungroup()        
        // already deduplicated
        .into_csv_in_dir(output, "s8_what_constitutes_software.csv")
}

#[djanco(April, 2021, subsets(Generic))]
pub fn s9_investigating_severity_thresholds(database: &Database, _log: &Log, output: &Path) -> Result<(), std::io::Error>  {
    database.projects()
        .filter_by(Matches(project::URL, regex!("https://github.com/(eclipse|apache)/")))
        .into_csv_in_dir(output, "s9_investigating_severity_thresholds.csv")
}

#[djanco(April, 2021, subsets(Generic))]
pub fn s10_investigating_severity_thresholds(_database: &Database, _log: &Log, _output: &Path) -> Result<(), std::io::Error>  {
    // Basically, can't do it
    unimplemented!()
}