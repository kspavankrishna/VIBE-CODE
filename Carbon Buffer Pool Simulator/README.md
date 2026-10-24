# Carbon Buffer Pool Simulator

A Fortran Monte Carlo tool that sizes the buffer pool behind nature based carbon credits. You give it a CSV of projects and it tells you how much to hold back so the pool survives fires, pests and droughts across a 40 year horizon, with correlated regional shocks included.

**Language:** Fortran | **Lines:** 784 | **Added:** 2026-10-24

## What this solves

Every forestry, mangrove and soil carbon programme has the same quiet problem. A credit says one tonne of CO2 was kept out of the atmosphere, but trees burn. When a project suffers a reversal, someone has to cancel an equal number of credits or the claim is false. The standard fix is a buffer pool: each project hands over a slice of its credits, the registry holds them, and reversals are paid out of that pool.

The hard question is how big the slice should be. Most programmes pick a flat percentage, often 10 to 20 percent, from a risk scorecard. That number is a policy choice, not a measurement. It says nothing about the chance the pool runs dry, and it hides the part that actually kills pools: losses are correlated. A drought year in one region raises fire risk for every project in that region at once. Ten projects each carrying a 1 percent annual risk do not behave like ten independent coin flips. They behave like one big coin that sometimes lands on all of them.

This tool answers the question properly. It simulates thousands of possible futures for your actual portfolio and finds, for each confidence level you ask for, the smallest opening pool that never runs out. It gives you a confidence interval on that number so you know how much of the answer is noise. It also tells you which regions drive the worst outcomes, which is usually not the region with the most stock.

If you run a registry, a project developer with a pooled portfolio, a carbon rating team or a buyer doing diligence on a seller's buffer, this is the number you have been guessing at.

## Why I built it

I work around carbon credit projects in South India and I keep seeing the same spreadsheet. A buffer percentage typed into a cell, a note saying "per methodology", and no way to ask what happens if two bad years land back to back in the same state. Rating agencies and buyers are now asking exactly that question, and a flat percentage cannot answer it.

I also wanted something that runs anywhere without a Python environment, a notebook or a licence. Fortran is a good fit. It is fast, it has no dependencies, gfortran is on every Linux box, and a million trials across 40 years and a hundred projects finish in seconds. The result is one source file you can read in an afternoon and audit line by line, which matters when the output feeds a financial claim.

The part I cared most about is repeatability. Auditors rerun things. If the same input and seed give a different answer on a laptop and on a 64 core server, nobody can trust the number. So every trial gets its own random stream derived from the seed and the trial number. Thread count, scheduling and even whether you compile with OpenMP make no difference. I checked: the serial build and the threaded build produce byte identical JSON.

## When to use it

Use it when you need to:

- Size a new buffer pool or challenge an existing flat percentage.
- Check that a pool you already have is big enough, and fail a CI job or a quarterly review if it is not (see `pool_pct` and `max_shortfall_prob`).
- Compare portfolios, for example before and after adding a project in a high risk region.
- Show a buyer or a rating team how the pool behaves in the tail, not just on average.
- Test how much a yearly top up from new issuance reduces the opening pool you need.

Do not use it as a replacement for a project level risk assessment. The hazard and severity numbers are inputs. If you feed in a hazard of 0.5 percent that came from nowhere, you get a precise answer to a made up question. The tool is only as honest as the CSV.

## How it works

The file has three parts: a random number module called `BufferRng`, a model module called `BufferModel` and the program `CarbonBufferPoolSimulator`.

**Random numbers.** `BufferRng` implements xoshiro256 starred starred. `SeedStream` takes the global seed and a trial number, runs them through a SplitMix64 mixer and fills the four state words. Each trial therefore owns an independent stream. `NextUniform` returns a value strictly inside (0,1) built from the top 53 bits plus a half step, so `log` never sees zero. `DrawBeta` builds a Beta variate from two gamma draws. `DrawGamma` is the Marsaglia and Tsang rejection method, with the usual boost for shape below one, a cap of `MaxRejection` attempts, and a fallback to the Beta mean if both gammas underflow on tiny shapes. The 64 bit mixing relies on wrap around integer arithmetic, which is why the build line includes `-fwrapv`.

**Input.** `LoadPortfolio` reads the CSV in two passes so arrays are sized exactly. Columns are found by header name through `ColumnIndex`, so order does not matter. It skips blank lines and lines starting with a hash, strips Windows line endings, rejects duplicate project ids, and checks every number with `IsNumber` and `ParseReal` before it reads anything. Stock must be positive, hazard must sit in [0,1] and both severity shape parameters must be above zero. Each failure names the line. `ParseArgs` validates every option the same way and exits with a distinct code: 2 for bad usage, 3 for bad data, 4 for a file problem and 1 for a failed gate.

**The model.** `SimulateTrial` walks one possible future year by year. At the start of each year it flips a regional shock coin for every region with probability `shock_prob`, and one global coin with probability `global_prob`. A project in a shocked region has its annual hazard multiplied by `shock_mult`, and the global shock multiplies by `global_mult`, capped at 1. If a project suffers a reversal, the fraction of its remaining stock lost is drawn from a Beta distribution set by `sev_alpha` and `sev_beta`. The project keeps what is left, so a project cannot lose more than it has and repeated hits get smaller. Dust below a billionth of the original stock is swept to zero.

The trick that keeps this cheap: losses do not depend on the pool size. So a trial records the running cumulative loss, subtracts the yearly top up, and tracks the worst deficit it ever reaches. That worst deficit is `requiredPool`, the smallest opening pool for which that trial never runs dry. One simulation answers every pool size at once.

**Reading the answer.** The program stores `requiredPool` for each trial, sorts a copy with `HeapSort` and picks the empirical quantile at each confidence level. The confidence interval on that quantile comes from order statistics using `Z95`, so you can see whether 200000 trials was enough. It also reports the tail mean, the average requirement among the trials beyond that quantile, which is the expected size of the bad outcome when the pool is breached.

**Tail attribution.** For the highest confidence level the program replays only the trials at or above that quantile. Because every trial has its own stream, the replay reproduces the exact same losses, and `regionLoss` shows how much of the tail each region contributed next to its share of stock. In the sample portfolio, California supplies about a third of the 99 percent tail on 20 percent of the stock. That is the kind of finding a flat percentage can never give you.

**Pool test.** If you pass `pool_pct`, the program converts it to tonnes, reports the probability of at least one shortfall with a 95 percent margin, the mean unmet reversal per trial and the median year of first shortfall. With `max_shortfall_prob` it exits with code 1 when the probability is above your limit.

## Usage

Build with OpenMP for speed:

```
gfortran -O2 -fopenmp -fwrapv -std=f2018 CarbonBufferPoolSimulator.f90 -o CarbonBufferPoolSimulator
```

A build without `-fopenmp` also works and gives identical results, only slower.

The input is a CSV with these header names: `project_id`, `region`, `stock_tonnes`, `annual_hazard`, `sev_alpha`, `sev_beta`. A working example is in `SamplePortfolio.csv`.

Run it:

```
./CarbonBufferPoolSimulator SamplePortfolio.csv years=40 trials=200000 confidence=0.95,0.99
```

Options, all written as `key=value`:

| Option | Default | Meaning |
|---|---|---|
| `years` | 40 | Horizon, 1 to 200 |
| `trials` | 200000 | Number of simulated futures, 1000 to 20000000 |
| `seed` | 20260101 | Global seed |
| `confidence` | 0.95,0.99 | Up to 8 levels, each above 0.5 and below 1 |
| `shock_prob` | 0.04 | Yearly chance a region has a bad year |
| `shock_mult` | 5 | Hazard multiplier in a bad regional year |
| `global_prob` | 0.01 | Yearly chance of a portfolio wide bad year |
| `global_mult` | 2 | Hazard multiplier in a global bad year |
| `topup` | 0 | Tonnes added to the pool each year |
| `pool_pct` | off | Test a pool of this percent of stock |
| `max_shortfall_prob` | off | Exit with code 1 if shortfall probability is above this |
| `format` | text | `text` or `json` |

A CI gate for a pool you already hold:

```
./CarbonBufferPoolSimulator SamplePortfolio.csv pool_pct=20 max_shortfall_prob=0.01 format=json
```

## Notes

- Set `shock_prob=0` and `global_prob=0` to see the independent case. Comparing it with your real settings shows how much correlation costs you. On the sample portfolio the 95 percent requirement moves from about 17.6 percent to about 19.3 percent of stock.
- The pool is treated as one account. There is no per project priority, no replenishment rule beyond `topup`, and no credit for projects that recover. Those are modelling choices you can change in `SimulateTrial`.
- Severity uses a Beta distribution on the share of remaining stock lost. Small `sev_alpha` and larger `sev_beta` give many small losses and the odd large one. Fit these to your own loss history where you have it.
- Shocks multiply hazard. They do not change severity. If your risk is that bad years also burn harder, raise severity in the CSV for the affected projects.
- Quantile confidence intervals describe Monte Carlo error only. They say nothing about whether your hazard inputs are right.
- Trial counts above a few million need memory for two arrays of that length. Region names are capped at 128 characters and tail attribution prints the top 10 regions.
- The numbers in `SamplePortfolio.csv` are illustrative. Do not read them as real project data.
