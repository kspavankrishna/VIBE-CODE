  slos {
    checkout-availability {
      objective 99.9
      period 30d
      errors {sum(rate(http_requests_total{job="checkout",code=~"5.."}[%WINDOW%]))}
      total  {sum(rate(http_requests_total{job="checkout"}[%WINDOW%]))}
      labels {team payments}
      min_rate 0.05
      budget_record yes
      runbook https://runbooks.example.com/checkout
    }
  }
