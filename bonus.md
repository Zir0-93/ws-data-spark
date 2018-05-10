
## Problem 4

### Part i)

- Let `Ad` be the Average Distance for a given POI
- Let `R` be the total number of requests for a given POI
- Let `Sdd` be the standard deviation distance for the POI
Then Mapping `M` for a given POI can be defined as:

```
M = -10 + (e^(-Ad + 2.3) + (R^(1/6))/2) + (Sdd^(1/3))/2)
```

Where M is the mapping for a given POI. This model is meant designed to focus on activity that is close to the POI:

1) A POI with a average distance close to 0 is a very strong indicator of increased popularity. As the distance moves
away, we would expect the resulting score to become significantly smaller. This component is reprensented by `e^(-Ad + 2.3)` which returns a value from 0-10 assuming the average distance value is positive.

2) However, a low average distance can result from a single point. We need to make sure there are a significant amount of requests. The
higher the amount of requests, the higher the score. This is represented by the component `(R^(1/6))/2`, which assigns a score from
 1-5 (assuming the maximum number of requests is capped at 1 million).

3) Lastly, the average distance could be low, but the number of request could be very large, and the standard deviation could be large
as well, which would mean there would still be a large amount of activity close to the POI. To capture this, we use the standard deviation to come up with a
value between 0 - 5 (assuming the maximum standard deviation is 1000 km) with the following component, `(Sdd^(1/3))/2`.

Part ii)

Not sure if this question is asking about POI's in general or the ones included in the data file. 
