package main

import (
  "flag"
  "fmt"
  "math"
  "os"
  "sort"
  "strings"
  "time"

  "github.com/aws/aws-sdk-go/aws"
  "github.com/aws/aws-sdk-go/aws/awserr"
  "github.com/aws/aws-sdk-go/aws/session"
  "github.com/aws/aws-sdk-go/service/cloudwatch"
  "github.com/guptarohit/asciigraph"
  "golang.org/x/crypto/ssh/terminal"
)

type Client struct {
  connection *cloudwatch.CloudWatch
}

func main() {
  metric, namespace, lookback, tail, err := parse()
  if err != nil {
    fmt.Println("Failed to parse args:", err.Error())
    return
  }

  client := createClient()
  client.renderMetricSampleCounts(metric, namespace, lookback, tail)
}

func parse() (string, string, time.Duration, bool, error) {
  lookbackPtr := flag.String("lookback", "-12h", "Amount of metric history to fetch")
  metric := flag.String("metric", "scheduled-charge-due-or-cdq-lte-30|updated", "Name of the metric to visualize")
  namespace := flag.String("namespace", "PlaidCron", "Namespace in which the metric exists")
  tail := flag.Bool("tail", false, "Tail metric, polling it every minute (the frequency w/ which metrics are updated)")
  flag.Parse()

  if !strings.HasPrefix(*lookbackPtr, "-") {
    *lookbackPtr = "-" + *lookbackPtr
  }
  lookback, err := time.ParseDuration(*lookbackPtr)
  if err != nil {
    fmt.Println("Failed to parse lookback:", err.Error())
    return *metric, *namespace, lookback, *tail, nil
  }
  
  return *metric, *namespace, lookback, *tail, nil
}

func createClient() Client {
  region := "us-east-1"
  sess := session.Must(session.NewSessionWithOptions(session.Options{
    SharedConfigState: session.SharedConfigEnable,
    Config: aws.Config{ Region: &region },
  }))

  return Client{ connection: cloudwatch.New(sess) }
}

func render(data []float64, metric string, namespace string, lookback time.Duration, end time.Time) error {
  width, height, err := terminal.GetSize(int(os.Stdin.Fd()))
  if err != nil {
    fmt.Println("Cannot fetch terminal size:", err.Error())
    return err
  }

  graph := asciigraph.Plot(
    data,
    asciigraph.Width(int(float64(width) * 0.98)),
    asciigraph.Height(int(float64(height) * 0.98)),
    asciigraph.Caption(fmt.Sprintf("[%s/%s] with lookback=%s (last updated at %s)", namespace, metric, lookback, end)),
  )
  asciigraph.Clear()
  fmt.Println(graph)

  return nil
}


func (client Client) renderMetricSampleCounts(metric string, namespace string, lookback time.Duration, tail bool) error {
  end := time.Now()
  start := end.Add(lookback)
  period := int64(60)
  sampleCount := cloudwatch.StatisticSampleCount
  request := cloudwatch.GetMetricStatisticsInput{
    MetricName: &metric,
    Namespace: &namespace,
    StartTime: &start,
    EndTime: &end,
    Period: &period,
    Statistics: []*string{ &sampleCount },
  }

  counts, err := client.getMetricSampleCounts(&request)
  if err != nil {
    return err
  }

  render(counts, metric, namespace, lookback, end)

  if tail {
    for {
      time.Sleep(time.Duration(1) * time.Minute)

      // Make new request
      start = end
      end = time.Now()
      newCounts, newErr := client.getMetricSampleCounts(&request)
      if newErr != nil {
        return newErr
      }
      counts = append(counts[len(newCounts):], newCounts...)

      renderErr := render(counts, metric, namespace, lookback, end)
      if renderErr != nil {
        return renderErr
      }
    }
  }

  return nil
}

func (client Client) getMetricSampleCounts(request *cloudwatch.GetMetricStatisticsInput) (counts []float64, err error) {
  datapoints, err := client.sendGetMetricStatisticsRequest(request)
  if err != nil {
    return counts, err
  }

  // Sort datapoints by timestamp
  sort.Slice(datapoints, func (i, j int) bool {
    return datapoints[i].Timestamp.Unix() < datapoints[j].Timestamp.Unix()
  })

  // Fill gaps w/ zeroes
  var previousTime *time.Time = request.StartTime
  for i := 0; i < len(datapoints); i++ {
    currentDatapoint := datapoints[i]
    numMinutesBetween := int(math.Round((currentDatapoint.Timestamp.Sub(*previousTime)).Minutes()))
    for j := 0; j < numMinutesBetween; j++ {
      counts = append(counts, 0)
    }
    counts = append(counts, *currentDatapoint.SampleCount)
    previousTime = currentDatapoint.Timestamp
  }

  return counts, nil
}

func (client Client) sendGetMetricStatisticsRequest(request *cloudwatch.GetMetricStatisticsInput) ([]*cloudwatch.Datapoint, error) {
  output, err := client.connection.GetMetricStatistics(request)
  if err != nil {
    if _, ok := err.(awserr.Error); ok {
      return client.splitGetMetricStatisticsRequest(request, 2)
    }
    return []*cloudwatch.Datapoint{}, err
  }

  return output.Datapoints, nil
}

// This will take a request and split it into n-many parallel requests to construct the output desired from the original request
func (client Client) splitGetMetricStatisticsRequest(request *cloudwatch.GetMetricStatisticsInput, parallelism int) ([]*cloudwatch.Datapoint, error) {
  fullStep := request.EndTime.Sub(*request.StartTime)
  splitStep, err := time.ParseDuration(fmt.Sprintf("%dm", int(math.Round(fullStep.Minutes())) / parallelism))
  if err != nil {
    return []*cloudwatch.Datapoint{}, err
  }

  splitRequests := make(chan []*cloudwatch.Datapoint)
  splitter := func (request cloudwatch.GetMetricStatisticsInput, start time.Time, end time.Time) {
    request.StartTime = &start
    request.EndTime = &end
    counts, err := client.sendGetMetricStatisticsRequest(&request)
    if err != nil {
      return
    }

    splitRequests <- counts
  }

  currentStepStart := *request.StartTime
  for i := 0; i < parallelism; i++ {
    splitStart := currentStepStart
    splitEnd := currentStepStart.Add(splitStep)

    go splitter(*request, splitStart, splitEnd)
    currentStepStart = splitEnd
  }

  datapoints := []*cloudwatch.Datapoint{}
  for i := 0; i < parallelism; i++ {
    requestResult := <-splitRequests
    datapoints = append(datapoints, requestResult...)
  }

  return datapoints, nil
}
