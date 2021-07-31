use anyhow::Result;
use nom::{
    branch::alt,
    bytes::complete::{tag, take_while},
    character::complete::multispace0,
    combinator::{map, opt},
    error::{ErrorKind, ParseError, VerboseError},
    multi::{many0, separated_list1},
    sequence::{delimited, pair, preceded, terminated},
    Compare, Err, FindSubstring, IResult, InputLength, InputTake, Slice,
};
use nom_locate::LocatedSpan;
use std::{fmt::Debug, ops::RangeFrom};

type Span<'a> = LocatedSpan<&'a str>;

fn take_until_or_eof<T, Input: InputLength, Error: ParseError<Input>>(
    tag: T,
) -> impl Fn(Input) -> IResult<Input, Input, Error>
where
    Input: InputTake + FindSubstring<T> + Debug,
    Error: Debug,
    T: InputLength + Clone,
{
    move |i: Input| {
        let t = tag.clone();
        let res: IResult<_, _, Error> = match i.find_substring(t) {
            None if i.input_len() > 0 => Ok(i.take_split(i.input_len())),
            Some(index) => Ok(i.take_split(index)),
            None => Err(Err::Error(Error::from_error_kind(i, ErrorKind::TakeUntil))),
        };
        res
    }
}

fn find_first<Input, T>(input: &Input, tags: &[T]) -> Option<(usize, usize)>
where
    Input: FindSubstring<T>,
    T: InputLength + Copy,
{
    tags.iter()
        .map(|t: &T| input.find_substring(*t))
        .enumerate()
        .filter_map(|(i, pos)| match pos {
            Some(p) => Some((i, p)),
            None => None,
        })
        .min_by_key(|(_i, pos)| *pos)
}

/// A parser similar to `nom::bytes::complete::take_until()`, but that does not
/// stop at balanced opening and closing tags. It is designed to work inside the
/// `nom::sequence::delimited()` parser.
///
/// It skips nested brackets until it finds an extra unbalanced closing bracket. Escaped brackets
/// like `\<` and `\>` are not considered as brackets and are not counted. This function is
/// very similar to `nom::bytes::complete::take_until(">")`, except it also takes nested brackets.
/// From https://gitlab.com/getreu/parse-hyperlinks
/// Copyright Jens Getreu
/// Licensed under either of Apache License, Version 2.0 or MIT license at your option.
fn take_until_unbalanced<T, Input, Error: ParseError<Input>>(
    opening_bracket: T,
    closing_bracket: T,
) -> impl Fn(Input) -> IResult<Input, Input, Error>
where
    Input:
        InputTake + Slice<RangeFrom<usize>> + InputLength + FindSubstring<T> + Compare<T> + Debug,
    T: InputLength + Clone + Copy,
    Error: Debug,
{
    move |i: Input| {
        let mut index = 0;
        let mut bracket_counter = 0;
        while let Some((which, n)) =
            find_first(&i.slice(index..), &[opening_bracket, closing_bracket])
        {
            index += n;
            match which {
                0 => {
                    bracket_counter += 1;
                    index += opening_bracket.input_len();
                }
                1 => {
                    bracket_counter -= 1;
                    index += closing_bracket.input_len();
                }
                _ => unreachable!(),
            }
            if bracket_counter == -1 {
                index -= closing_bracket.input_len();
                let value = Ok(i.take_split(index));
                return value;
            };
        }
        if bracket_counter == 0 {
            Ok(i.take_split(0))
        } else {
            Err(Err::Error(Error::from_error_kind(i, ErrorKind::TakeUntil)))
        }
    }
}
#[derive(Debug, Eq, PartialEq)]
pub enum JoinedStringPart<'a> {
    Constant(Span<'a>),
    Expression(Span<'a>),
}

impl<'a> JoinedStringPart<'a> {
    fn fragment(&self) -> &'a str {
        match self {
            JoinedStringPart::Constant(s) => s.fragment(),
            JoinedStringPart::Expression(s) => s.fragment(),
        }
    }
}

#[derive(Debug, Eq, PartialEq)]
pub struct BangCommand<'a> {
    pub preamble: Vec<Span<'a>>,
    pub body: Vec<JoinedStringPart<'a>>,
}

impl<'a> BangCommand<'a> {
    #[allow(dead_code)]
    fn body_fragments(&'a self) -> Vec<&'a str> {
        self.body.iter().map(|f| f.fragment()).collect()
    }

    pub fn preamble_fragments(&'a self) -> Vec<&'a str> {
        self.preamble.iter().map(|f| *f.fragment()).collect()
    }
}

pub fn parse_fstringish(i: &str) -> Result<BangCommand, nom::Err<VerboseError<Span>>> {
    fn delim_expr(i: Span) -> IResult<Span, JoinedStringPart, VerboseError<Span>> {
        let inner_expression = map(take_until_unbalanced("{", "}"), |s: Span| {
            JoinedStringPart::Expression(s)
        });
        let res = delimited(tag("${"), inner_expression, tag("}"))(i);
        res
    }

    let const_expression = map(take_until_or_eof("${"), |s: Span| {
        JoinedStringPart::Constant(s)
    });
    let preamble_expression = map(
        opt(delimited(
            terminated(tag("["), multispace0),
            separated_list1(
                delimited(multispace0, tag(","), multispace0),
                take_while(|s: char| s.is_alphanumeric() || s == '='),
            ),
            preceded(multispace0, tag("]")),
        )),
        |x: Option<Vec<Span>>| match x {
            None => vec![],
            Some(v) => v,
        },
    );

    let mut parser = map(
        pair(
            preamble_expression,
            many0(alt((delim_expr, const_expression))),
        ),
        |(preamble, body)| BangCommand { preamble, body },
    );

    let span = Span::new(i);
    match parser(span) {
        Ok((_remaining, value)) => Ok(value),
        Err(e) => Err(e),
    }
}

#[test]
fn test_0() -> Result<()> {
    let value = parse_fstringish("")?;
    assert_eq!(
        value,
        BangCommand {
            preamble: vec![],
            body: vec![]
        }
    );
    Ok(())
}

#[test]
fn test_1() -> Result<()> {
    let value = parse_fstringish("${inner}")?;
    assert_eq!(value.preamble.len(), 0);
    assert_eq!(value.body_fragments(), vec!["inner"]);
    Ok(())
}

#[test]
fn test_2() -> Result<()> {
    let value = parse_fstringish("${{inner}}")?;
    assert_eq!(value.body_fragments(), vec!["{inner}"]);
    Ok(())
}

#[test]
fn test_3() -> Result<()> {
    let value = parse_fstringish("aa ${ [inner] } bb")?;
    assert_eq!(value.body_fragments(), vec!["aa ", " [inner] ", " bb"]);
    Ok(())
}

#[test]
fn test_4() -> Result<()> {
    let value = parse_fstringish("aa ${ ${} } bb")?;
    assert_eq!(value.body_fragments(), vec!["aa ", " ${} ", " bb"]);
    Ok(())
}

#[test]
fn test_6() -> Result<()> {
    let value = parse_fstringish("[a, b, c=1]")?;
    assert_eq!(value.preamble_fragments(), vec!["a", "b", "c=1"]);
    Ok(())
}

#[test]
fn test_7() -> Result<()> {
    let value = parse_fstringish("[a, b, c=1] ${a} bb ${{}}c")?;
    assert_eq!(value.preamble_fragments(), vec!["a", "b", "c=1"]);
    assert_eq!(value.body_fragments(), vec![" ", "a", " bb ", "{}", "c"]);
    Ok(())
}

#[test]
fn test_8() -> Result<()> {
    let value = parse_fstringish("[ a,b,c=1 ] ${a} bb ${{}}c")?;
    assert_eq!(value.preamble_fragments(), vec!["a", "b", "c=1"]);
    assert_eq!(value.body_fragments(), vec![" ", "a", " bb ", "{}", "c"]);
    Ok(())
}
