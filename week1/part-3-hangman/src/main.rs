// Simple Hangman Program
// User gets five incorrect guesses
// Word chosen randomly from words.txt
// Inspiration from: https://doc.rust-lang.org/book/ch02-00-guessing-game-tutorial.html
// This assignment will introduce you to some fundamental syntax in Rust:
// - variable declaration
// - string manipulation
// - conditional statements
// - loops
// - vectors
// - files
// - user input
// We've tried to limit/hide Rust's quirks since we'll discuss those details
// more in depth in the coming lectures.
extern crate rand;
use rand::Rng;
use std::collections::HashSet;
use std::fs;
use std::io;
use std::io::Write;

const NUM_INCORRECT_GUESSES: u32 = 5;
const WORDS_PATH: &str = "words.txt";

fn pick_a_random_word() -> String {
    let file_string = fs::read_to_string(WORDS_PATH).expect("Unable to read file.");
    let words: Vec<&str> = file_string.split('\n').collect();
    String::from(words[rand::thread_rng().gen_range(0, words.len())].trim())
}

fn main() {
    let secret_word = pick_a_random_word();
    // Note: given what you know about Rust so far, it's easier to pull characters out of a
    // vector than it is to pull them out of a string. You can get the ith character of
    // secret_word by doing secret_word_chars[i].
    let secret_word_chars: Vec<char> = secret_word.chars().collect();
    // Uncomment for debugging:
    println!("random word: {}", secret_word);

    // Your code here! :)
    println!("Welcome to CS110L Hangman!");
    let word_len = secret_word_chars.len();
    let mut remain_len = word_len;
    let mut found_word = vec!['-'; word_len];
    let mut guesses = String::new();
    let mut num_guess = NUM_INCORRECT_GUESSES;
    let mut found_char: HashSet<char> = HashSet::new();
    loop {
        let word_str: String = found_word.clone().into_iter().collect();
        println!("The word so far is {:?}", word_str);
        println!("You have guessed the following letters: {:?}", guesses);
        println!("You have {:?} guess left", num_guess);

        print!("Please guess a letter: ");
        io::stdout().flush().expect("Error flushing stdout.");
        let mut guess = String::new();
        io::stdin()
            .read_line(&mut guess)
            .expect("Error reading line.");

        let guess_char = guess.chars().next().unwrap();

        let find_index: Vec<usize> = secret_word_chars
            .iter()
            .enumerate()
            .filter_map(|(index, &ch)| if ch == guess_char { Some(index) } else { None })
            .collect();

        let find_num = find_index.len();

        if find_num == 0 {
            num_guess -= 1;
            println!("Sorry, the letter is not in the word");
        } else if found_char.insert(guess_char) {
            for index in find_index {
                found_word[index] = guess_char;
            }
            remain_len -= find_num;
        }

        guesses.push(guess_char);

        if num_guess == 0 {
            println!("Sorry, you ran out of guesses!");
            break;
        }

        if remain_len == 0 {
            println!(
                "Congratulations you guessed the secret word: {:?}!",
                secret_word
            );
            break;
        }
    }
}
