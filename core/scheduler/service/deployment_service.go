package service

import (
	"context"
	"fmt"
	"strings"
	"time"

	"go.opentelemetry.io/otel"
	"k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/tools/cache"

	"github.com/goto/optimus/core/job"
	"github.com/goto/optimus/core/scheduler"
	"github.com/goto/optimus/core/tenant"
	"github.com/goto/optimus/internal/errors"
	"github.com/goto/optimus/internal/utils"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/clientcmd"
)

//func (s *JobRunService) StreamLogsOfContainer(ctx context.Context) {}

func (s *JobRunService) RunJob(ctx context.Context) {

	// Build the config from the kubeConfig path
	kubeConfig := `
apiVersion: v1
clusters:
- cluster:
    certificate-authority-data: LS0tLS1CRUdJTiBDRVJUSUZJQ0FURS0tLS0tCk1JSUJlRENDQVIyZ0F3SUJBZ0lCQURBS0JnZ3Foa2pPUFFRREFqQWpNU0V3SHdZRFZRUUREQmhyTTNNdGMyVnkKZG1WeUxXTmhRREUzTURnMk1ESXpNVFF3SGhjTk1qUXdNakl5TVRFME5URTBXaGNOTXpRd01qRTVNVEUwTlRFMApXakFqTVNFd0h3WURWUVFEREJock0zTXRjMlZ5ZG1WeUxXTmhRREUzTURnMk1ESXpNVFF3V1RBVEJnY3Foa2pPClBRSUJCZ2dxaGtqT1BRTUJCd05DQUFRL2JUdEtFdlErektWbE9qc3lSaGdITThmQitaL21JdWtOakE2TEU1ZS8KYWZZTEd2NThneGRGemkvZjRyM0RyV2lsS3pTckFlRjE0OW1NSU5wR1hJWUNvMEl3UURBT0JnTlZIUThCQWY4RQpCQU1DQXFRd0R3WURWUjBUQVFIL0JBVXdBd0VCL3pBZEJnTlZIUTRFRmdRVWYzc05qUDhGZCtVOXZsNWxoSHZMClVzMVhYaDR3Q2dZSUtvWkl6ajBFQXdJRFNRQXdSZ0loQU0xL2xKN1ZmekQzTFRYR1VoSy9GczgxSTdhQjJjMW0KOVEyTmxpK2V2Y0d0QWlFQW84NGY5b2x3OTdPYlNBTHM5cWw1dEVTZ0JwRVFwYmJQTXdkZGpXeW5GSjg9Ci0tLS0tRU5EIENFUlRJRklDQVRFLS0tLS0K
    server: https://127.0.0.1:6443
  name: colima
- cluster:
    certificate-authority-data: LS0tLS1CRUdJTiBDRVJUSUZJQ0FURS0tLS0tCk1JSUVMRENDQXBTZ0F3SUJBZ0lRU240QmRPQW9pR01JNHEwUVZYYzBuVEFOQmdrcWhraUc5dzBCQVFzRkFEQXYKTVMwd0t3WURWUVFERXlSbVlUYzRPVGd5WXkweFlqVmxMVFJqWldFdFlqSTNOeTFrTnpJMk56azJOelUxTlRVdwpJQmNOTWpNd05EQTJNRGt4TVRFeldoZ1BNakExTXpBek1qa3hNREV4TVROYU1DOHhMVEFyQmdOVkJBTVRKR1poCk56ZzVPREpqTFRGaU5XVXROR05sWVMxaU1qYzNMV1EzTWpZM09UWTNOVFUxTlRDQ0FhSXdEUVlKS29aSWh2Y04KQVFFQkJRQURnZ0dQQURDQ0FZb0NnZ0dCQU1qbE15ZTlMQkozRzdXdEhBenRBazl6MlBoZ1krendBRnNGSmYvTAphRkRqb3I3YUtlUVJudHJvd1gxZDhHdmo5emt1QjZnWFNGL25GOE5XS01BVXpTZnk1RXlTZ1g2UnNtU0NMa1ZkCkh1N0w0eENmNDJzc1VFL05oSjBtLzNEUTBHd3FLSW1WTTVvTGhZT3FsNEp2b29FaTNmbEx3R0M5RTBxaThrMWUKSEk0U1dtUFNOOFQ4bS9BcEsyZjZFakhXM2tBeldLVWUxNTdKV01CT1JxRU5MSFU3WW9DT1RDUEhub0ZzdFFJdApZaXpRWUZlUDZJSktodDZmNU9CaE1KVC80MXV4SGlsRHczcUFqSHh4Ukx1YXhkbGprQ1BXYWNMSnY1cStScDdzCnFqbzBuN2xpYWtyQzhlNkJNZndBV1RlaTV0V3VEWlRyMWtzT251YXNlaXJqUzRRZzZMNjB6UU4rbWxNVEJjUUQKSHU2VFkvT29RaFJmdVpTL09CT2V2cHJxWWVlSUx5USsyVGtOdGJLY2lGbmFINTZyUVgwclUvUS9HdXVEcjBZUQpZKyt0Z1RDY3hPVHlJdWpsU1lXUHh4REJ5N2UvazdDVGhPZGNKa05oa0ptRWFJZU1WSW5OUzJNc1J1amdWY2plCkVzOWcwK0tFVjhEc081TUlibFg4a3RrUEt3SURBUUFCbzBJd1FEQU9CZ05WSFE4QkFmOEVCQU1DQWdRd0R3WUQKVlIwVEFRSC9CQVV3QXdFQi96QWRCZ05WSFE0RUZnUVV4MmI5ZlNOVVVYbXRpcUVDMHNWMnFURDdJT293RFFZSgpLb1pJaHZjTkFRRUxCUUFEZ2dHQkFHZndETFg3Z25ZRk1UQnJMREtjNEZBaER1MTVnVTV4SnhTNk5WN05YdkM3Ck1KTTJrQU5sMStVMGJLL0VqcW1SWkxMeVY4UWl5UGE0aHpreWdSZjFNNFBBZ0dtWmMzc3REME9pZjdsbHF3cW8KZURCcEtlMGlkdlpiQnM1MUtQMVpJY0tmOGVLMjVNaENmd1BEaWpMUnkrMEtObk1Yc29SaVMvcksxK2w5ZUhpZwphSGxsZUI2L0gvQ0J2ZnQxODFlT0d0aTlrR2JYVVVQc0dXNUR1V2ZiQUlWREQyWDhuUU44WFIrenlXaFdzbW44CnV5SlQwSlJTQzI4TnFOOHREVTZwdjhGd0Z2NHFMb3pKRWt1R29ITGk4TTd6eVlQZDJleWdoQUZWNDYwV2ZubDEKWkdjTnpVOXJnU2VPTXF5aGlHWGI3ZGNWTWdMTXVneWhXVithem5xR1pudzRma2t6b1NvZDZXVVFkcmRuejR0SgpDMFc5aDZseFhZVnB4eWQyZ3JIWWZFRzZZQ2UwZnlHaXZFZ3grLzZhYWFLOXBMamY2ZU8zWWNzUFNta2tSVkJ6CkVLeDlkSUw2TjlHUUh0dFdJdVJFdVBzWWVlUlVqczhxcHE2a1FZZmh4QTdWVjNzU3lDMnhrU1dZazhiN3RkcTYKQ2k4RndJTy9qR2xkUWkzS1lQVWRzUT09Ci0tLS0tRU5EIENFUlRJRklDQVRFLS0tLS0K
    server: https://10.255.253.194
  name: gke_godata-integration_asia-east1_g-godata-id-sourcing
- cluster:
    certificate-authority-data: LS0tLS1CRUdJTiBDRVJUSUZJQ0FURS0tLS0tCk1JSURDekNDQWZPZ0F3SUJBZ0lRR0N5S2hNVk1UL1pyOFpuYldVZFBDakFOQmdrcWhraUc5dzBCQVFzRkFEQXYKTVMwd0t3WURWUVFERXlRM1pUTm1PVGMzWXkwMVpEUXlMVFF3Wm1NdFlqVmtPUzA0T1dObU1qSmxNMkV5TldVdwpIaGNOTVRreE1qQTBNRGt4TnpFeVdoY05NalF4TWpBeU1UQXhOekV5V2pBdk1TMHdLd1lEVlFRREV5UTNaVE5tCk9UYzNZeTAxWkRReUxUUXdabU10WWpWa09TMDRPV05tTWpKbE0yRXlOV1V3Z2dFaU1BMEdDU3FHU0liM0RRRUIKQVFVQUE0SUJEd0F3Z2dFS0FvSUJBUUMzR3c5akJlS0hzUFBwYzc3UkcwOEcxdkh0Y0t4M29ZcCtoNVA1MzFxQgplU093UDVORy8rY2dTaThQeXlTTG15Z2cwT2tmNXJxVzRCd2ZVMUJ0cU9YL1phMk03WUUxdnBlUWQvbnQ4UkN3ClZ3Z0tKT0RQNjUzQzhjMUxnN0s3SXFuVGl5eXRCcFhCckw2OWpvRTl2VHF2RXpIbHZrSklMOW5jazZvUlp1Q1IKZWtieks1dnM2VC8rSnROQ2lEUWZBcCtneFBlakZIYVRwNUVlNnZJTlEwTGRLRzB4RGZ2dmxXdDV1NXAxZnF6MApiTG1laDFuK0lSKytFdkhQOHJoaGU5UVlKcjZxbjJEVjlIS2hWb051cHN6eG5nTG5DS0tnN1NFU0w5ZUI3Und2CnZCNW9rQjRqTkk4b3ZHYXUwOFFYdDBaNWRXalRrTitGalN1cDVQZHNDM1YzQWdNQkFBR2pJekFoTUE0R0ExVWQKRHdFQi93UUVBd0lDQkRBUEJnTlZIUk1CQWY4RUJUQURBUUgvTUEwR0NTcUdTSWIzRFFFQkN3VUFBNElCQVFBSgorTXJOWEpiVEJsYVozV2RFRGNwQTVLQ3JKaFVJUjgvay9idCt1WmhXTU15ZVpoclBJN09GL3FJUnRGNnU2TXdvClRmaXZpTTlNck9EZHVwNGJCcUExR05KdWhjZklXcmp3cVcyam1KVE5iUjhIaWpwSWkzbXJCZkxSMWwvcUJFSDQKeHFXNnBiRWJYVEttQkx0L1ZYcU1IcGhydU52VXoybHpkbmYwOHM2ZTJRSTZ1U0Y1MSs2Zlg3Zi9KUlArODRVRApXQ2dmUWhNOUJPWDVlR1ZUL25FMVZvVnFqQmdMeEZVRmVqLzdHbmEzUTZVTVdDcnlQRW83S2lmblZ0YW1zU2lHClFLU0JXTXF4aFh5dTBtZ2RIa21DQldqRFBtWENwK1dIMGttczFmVzk2MWIwVEZLK0Y1Rm1jZW5HNWpiZ0NmeFgKZFZlZUhtVnNYakxqMnRyeEwvNjAKLS0tLS1FTkQgQ0VSVElGSUNBVEUtLS0tLQo=
    server: https://10.255.255.130
  name: gke_godata-platform_asia-southeast1_g-godata-systems-console
- cluster:
    certificate-authority-data: LS0tLS1CRUdJTiBDRVJUSUZJQ0FURS0tLS0tCk1JSURDekNDQWZPZ0F3SUJBZ0lRZVF0MDhCTUdYbHZqT2N4aGxEd2o4VEFOQmdrcWhraUc5dzBCQVFzRkFEQXYKTVMwd0t3WURWUVFERXlRM00yUXlNRFE0TnkwMFkyRXhMVFF4TXpVdE9XSXhPQzFqWWpGbE9EZGxZakZsWkRVdwpIaGNOTVRreE1qQTBNVEF5TWpRNFdoY05NalF4TWpBeU1URXlNalE0V2pBdk1TMHdLd1lEVlFRREV5UTNNMlF5Ck1EUTROeTAwWTJFeExUUXhNelV0T1dJeE9DMWpZakZsT0RkbFlqRmxaRFV3Z2dFaU1BMEdDU3FHU0liM0RRRUIKQVFVQUE0SUJEd0F3Z2dFS0FvSUJBUUM3YU93NjZJUkJOdWJ6OE1LWHRIdUJYU2tNdFlUUHlIZDgrMXZkRmlpbgplSEhtMnU0Q0RsNERpd0loM3ZkMVJJcnJvZVBKSDhnSkdQbC8xY0ZhQktBczBTQ0taZVdwT1FsdE1LN0U0NVplCjVDNDRpNUJtaGJQell1M2xaMDJXWEJXQ3Y2dTl1a2syd0cvenJTV3M0KzE4bk8zQ0ZvUHpJL2FaQzJPOTBpc1oKMGFzNGVuOVRTczV0d0RqV2lJbnRESWR5djNYZG5GUDJmY1M1cVR0MjlrbHRJOXFoZlRQd2dyU29yUFlUd250ZgpLOXdzbk12ZzRyYnpDbEVUeEh3Tm5ISTNrRjRNMWNqTnFySkRnMWd5MTFldmxRT0p0NUN2MHBxVW1pcC9SSURKCjFmQzNWcXQ3SXJXZkVaS3VCWDNOWVBUNlhqU1hsMGFoRDJscFRzMTgza2hQQWdNQkFBR2pJekFoTUE0R0ExVWQKRHdFQi93UUVBd0lDQkRBUEJnTlZIUk1CQWY4RUJUQURBUUgvTUEwR0NTcUdTSWIzRFFFQkN3VUFBNElCQVFBZQpPSWxFWTFuemt0Mm1pRnMrNDBLTDJkSXRSOVZHR05YWGNTVEs3Si9oZmJXNERtQ0JNU1BxOXFBMjJBN2R6WUVGCmlNcnc1UEdqSkcxcjM1cXNKbUJMVGdJRFFBNEdSbXAwVzZ0QnplMEtHVFNoYmZSV2tIeGRmbm9qcGxrWGZKcVAKTGlVQXYxK3V2ME9TeE4weFBob2FHUHBCOUF6bXRLR2tFWmd0R2YrTWdxZnp0Ky8yUDlJTHZCZ1ZFdFVnSTJxTApKY1JPMEZPdW80WmR4anBBc20rS21TdXFKbzNrcnFXdytFYzRIR3ovUnQwRUR3ekRabGZIaExreXk3RjNLS2t6CklXNE15RCthOUVCaVluUFlmelpCdkUrMDM4c3FmNjZjRVJtZHdFbnZJejNBVTU2SnVobzJRaE5VWjdtaVE5bU8KeVZTNW9sTCtUbjQ4eG14dnlBSk0KLS0tLS1FTkQgQ0VSVElGSUNBVEUtLS0tLQo=
    server: https://10.255.255.146
  name: gke_godata-platform_asia-southeast1_p-godata-systems-console
- cluster:
    certificate-authority-data: LS0tLS1CRUdJTiBDRVJUSUZJQ0FURS0tLS0tCk1JSURERENDQWZTZ0F3SUJBZ0lSQU1hc3RUSmJlU1c4VEVpOWJZd1VDT2N3RFFZSktvWklodmNOQVFFTEJRQXcKTHpFdE1Dc0dBMVVFQXhNa056aGpORFZrTXpjdE5tUmtPUzAwWXpGa0xUaGlaR1l0WmpFNU5tUTNaVFV6TlRobApNQjRYRFRFNU1UQXhOakEwTlRBMU5Wb1hEVEkwTVRBeE5EQTFOVEExTlZvd0x6RXRNQ3NHQTFVRUF4TWtOemhqCk5EVmtNemN0Tm1Sa09TMDBZekZrTFRoaVpHWXRaakU1Tm1RM1pUVXpOVGhsTUlJQklqQU5CZ2txaGtpRzl3MEIKQVFFRkFBT0NBUThBTUlJQkNnS0NBUUVBNWZoeHl5TS83UUxDa1VacVdaWXQ1N0pkYzdYRDBET1pSdThkUDVvWQpzTGpVTDBlbTBWTFZsR3Q1aVAyTCswSmVkQ095ZktESmY2d3UzLzNEVHhaanN1UVE2M00rTjEwdW1lZ2d3anN3Ck5DM3l1N01XbllpUkFCNjA2THkwOHpkZnNCdmo3NVUrbWN4bmVFWFdsT2ZHK1Z0VXZBaDNJWGVDc3pjL0RVRDYKN1gxdHdsaStuaUpxaUFUS2h3b1VEdXhBb2R2LzFSYVlWN29uTlVwQkhaT2U1UkIveHh1MWh1RlJLbCtISkJkNwpQb0EwbWdzZmhNeWhGKzVVNG5OVTloS2xDM2FCWkRySCtQQjh3TWNFT3dwVEt5Zzh6WkZkNDU2S0NYRXhYZ2dwCmNWTXpyNE5RcnR1WFhQcFdzOVJkbGN4enFvWFdaejBaN2JlRjlzajFxa3Bhc1FJREFRQUJveU13SVRBT0JnTlYKSFE4QkFmOEVCQU1DQWdRd0R3WURWUjBUQVFIL0JBVXdBd0VCL3pBTkJna3Foa2lHOXcwQkFRc0ZBQU9DQVFFQQppeDFUVUpIR1FUeTQrdHpodnBmWmxXTVBGS05XV0F5T3NrekRYcDhaWDlPTng3SHpoVzJjTTVRdG0rZ1huWGorCnp3M1cwdlFzblpoQzYwakZ3Z1hnNEtPc3hVanhicUVRZm10dTNEMWJlcUVRaGZkaVpJT3liNWNuVjJvQkVkRnUKY2c5L1B5MEZCUzEzZlVsRWlXWWZOaHpwdDQ0RldERjNxZGgzbXlvNjErTmRNZU02Ryt6Z245REUyWCt3c2JnRQpTUnZjNmlTNUwyOU5vN1lVVUlRWkN3c2Vua3ZvZHZrNE9mM0tXM0RyTHhjRUM2Qk1WWlFxMW15RFlEVnNvYlNDCkJYRFM1WlhRZ2NtelVGUmlUNEt6cXFJbzZpUng1c2dFWVpFQ20weEFvbmJmRU03RVg2Nk9LTmQvVkNBNVlEM08KcTFjZHZiUERTeW1FUmM5aEdNTDJqUT09Ci0tLS0tRU5EIENFUlRJRklDQVRFLS0tLS0K
    server: https://10.255.253.114
  name: gke_godata-production_asia-east1_p-godata-id-batching
- cluster:
    certificate-authority-data: LS0tLS1CRUdJTiBDRVJUSUZJQ0FURS0tLS0tCk1JSURLekNDQWhPZ0F3SUJBZ0lSQUtjZFJ0TEJuZXBBOWNBc2g2ZkdjRUV3RFFZSktvWklodmNOQVFFTEJRQXcKTHpFdE1Dc0dBMVVFQXhNa1pUQmxOelpoTjJJdE1HWXhaQzAwTVRjNExUazJOMk10WlRoa05HSXpaakkwTldVNApNQjRYRFRJd01UQXlOakF6TlRneU1Gb1hEVEkxTVRBeU5UQTBOVGd5TUZvd0x6RXRNQ3NHQTFVRUF4TWtaVEJsCk56WmhOMkl0TUdZeFpDMDBNVGM0TFRrMk4yTXRaVGhrTkdJelpqSTBOV1U0TUlJQklqQU5CZ2txaGtpRzl3MEIKQVFFRkFBT0NBUThBTUlJQkNnS0NBUUVBbFRrRzcvNE9PNEo4OEtXbXFwbnZIRHc3UkFMUlcxd1NYeVFER2hZawpWZWRSbGZzQS9xZnJtL2VVbzFDVFAxT0FDTituTFVwU0V4UEJpU3BYV1BRYjd3dGt2ekcxL0RQKytjQStMM1Q5CjZmckJtTEtSSDZNTDVHdHVWL1BTNzhmKzBaQkR0SmNDdWFzekl4Q1JnbEFwMEJZRlg4Ymd3R0QxTmpLVUg2bkMKYnBLWXBUK2ZGQzNUTlR3MkxIeCtSZytIQzdQdTcvRm00RnFRS0xnRUNYVm5Tcks3K0JDajRlWnVncko4TU0wagpVYTYydkxoa2ZqSDlRL3l4UHVTMVdkMVFwMFlyL1ZzWmg5MlJrN2VtY1E2eXZhTXB0K3BYYzNkMVhFekphRFAzCnVkdysyRkFaZVcxRGF0WjhRVVZ5c3BPWUJPSnNSNmtPd2lRVFVTM25iRHpwd1FJREFRQUJvMEl3UURBT0JnTlYKSFE4QkFmOEVCQU1DQWdRd0R3WURWUjBUQVFIL0JBVXdBd0VCL3pBZEJnTlZIUTRFRmdRVWFGV0s5REIvNDR2UApZZFF3SWFHL3o1YUZZZzR3RFFZSktvWklodmNOQVFFTEJRQURnZ0VCQUJSNHhYL0FhRHhMZ3hydDVSZDZabjBrCmVLemYzcFVpcEZPYzFMUDZ5OEZmR1JscVJ0UWJlN3NSd1c1M09rMHI0dk1GSDNNY3J2KzBsM2dvdWlrd0JxR1IKNEk3a25UNGlvWkg0QVY3bFVhVVVNOHpCTFh1U0VTalBlZVo2YXFoY0ZueWNXL0lYdWx6cGFjbXhITm5LSXN3aQp2bC9wL0tkM1Zmakh5TGlUVUk5Ylh3TzNsRE9SbFlPTEg2TDd5ZGw1ZHRtUmM3cVBIbkdXMm81LzNJVTVINkJyCkNuWHQ2YlI4YzlOT0N6TGtmYVUvVWRKSjRpeXpKZ1Bvb09yaUUwOHlmbG5yaVRwZWpXZmdGTEhxSkpCaUJReTIKYy85MHVyOGc1OFZzS0hqN20rbTRGcC9GY3VWOHl6NVFVL0V2Q21McXZXTCtOMXRocU04bTg3cjNGVmxBc1hVPQotLS0tLUVORCBDRVJUSUZJQ0FURS0tLS0tCg==
    server: https://10.109.129.130
  name: gke_pilotdata-integration_asia-southeast1_g-pilotdata-gl-sourcing
contexts:
- context:
    cluster: colima
    user: colima
  name: colima
- context:
    cluster: gke_godata-integration_asia-east1_g-godata-id-sourcing
    user: gke_godata-integration_asia-east1_g-godata-id-sourcing
  name: gke_godata-integration_asia-east1_g-godata-id-sourcing
- context:
    cluster: gke_godata-platform_asia-southeast1_g-godata-systems-console
    user: gke_godata-platform_asia-southeast1_g-godata-systems-console
  name: gke_godata-platform_asia-southeast1_g-godata-systems-console
- context:
    cluster: gke_godata-platform_asia-southeast1_p-godata-systems-console
    namespace: optimus
    user: gke_godata-platform_asia-southeast1_p-godata-systems-console
  name: gke_godata-platform_asia-southeast1_p-godata-systems-console
- context:
    cluster: gke_godata-production_asia-east1_p-godata-id-batching
    user: gke_godata-production_asia-east1_p-godata-id-batching
  name: gke_godata-production_asia-east1_p-godata-id-batching
- context:
    cluster: gke_pilotdata-integration_asia-southeast1_g-pilotdata-gl-sourcing
    user: gke_pilotdata-integration_asia-southeast1_g-pilotdata-gl-sourcing
  name: gke_pilotdata-integration_asia-southeast1_g-pilotdata-gl-sourcing
current-context: colima
kind: Config
preferences: {}
users:
- name: colima
  user:
    client-certificate-data: LS0tLS1CRUdJTiBDRVJUSUZJQ0FURS0tLS0tCk1JSUJrRENDQVRlZ0F3SUJBZ0lJV09BbGJvM0g4Q2d3Q2dZSUtvWkl6ajBFQXdJd0l6RWhNQjhHQTFVRUF3d1kKYXpOekxXTnNhV1Z1ZEMxallVQXhOekE0TmpBeU16RTBNQjRYRFRJME1ESXlNakV4TkRVeE5Gb1hEVEkxTURJeQpNVEV4TkRVeE5Gb3dNREVYTUJVR0ExVUVDaE1PYzNsemRHVnRPbTFoYzNSbGNuTXhGVEFUQmdOVkJBTVRESE41CmMzUmxiVHBoWkcxcGJqQlpNQk1HQnlxR1NNNDlBZ0VHQ0NxR1NNNDlBd0VIQTBJQUJEMTg2RndZUHI1Ulh1VTAKanQxNnV5R1pocGdvRkw5WHd1YVVwM0daekVyMEhsN2hVSW1YbWRRYmllUzhiNXhEdnozbGNjN2ZPVWd1OFkzRApiMWpMYTFDalNEQkdNQTRHQTFVZER3RUIvd1FFQXdJRm9EQVRCZ05WSFNVRUREQUtCZ2dyQmdFRkJRY0RBakFmCkJnTlZIU01FR0RBV2dCUkE4emtQaXlUTmlwRmZGNTcvNlJoT09DQkpNVEFLQmdncWhrak9QUVFEQWdOSEFEQkUKQWlBeU5pRWdvaDcwcnd4WDUwTWZrdmV4ZmduRlRHc1RBSmRkcG1yL3RGRGFsUUlnY3BPNDl5bVBFam5zakw0SQpycnhlQzRxUVEyeUdTL1JSWGVRUkxaOUtCMWc9Ci0tLS0tRU5EIENFUlRJRklDQVRFLS0tLS0KLS0tLS1CRUdJTiBDRVJUSUZJQ0FURS0tLS0tCk1JSUJkekNDQVIyZ0F3SUJBZ0lCQURBS0JnZ3Foa2pPUFFRREFqQWpNU0V3SHdZRFZRUUREQmhyTTNNdFkyeHAKWlc1MExXTmhRREUzTURnMk1ESXpNVFF3SGhjTk1qUXdNakl5TVRFME5URTBXaGNOTXpRd01qRTVNVEUwTlRFMApXakFqTVNFd0h3WURWUVFEREJock0zTXRZMnhwWlc1MExXTmhRREUzTURnMk1ESXpNVFF3V1RBVEJnY3Foa2pPClBRSUJCZ2dxaGtqT1BRTUJCd05DQUFRcXJNRGNqeTB0eTJCMmxrcG5Vdmx6U1VHQTlVWU81YW1NMEp2WC9LMEwKdlNISzZaUnQzYS9MRGdPbUl4ZmYwaGJaL0JaN0hsNnZmdXJFR0MwTXVoM2NvMEl3UURBT0JnTlZIUThCQWY4RQpCQU1DQXFRd0R3WURWUjBUQVFIL0JBVXdBd0VCL3pBZEJnTlZIUTRFRmdRVVFQTTVENHNrellxUlh4ZWUvK2tZClRqZ2dTVEV3Q2dZSUtvWkl6ajBFQXdJRFNBQXdSUUloQVBnOHJ4elBYbU9uU0N6TlN5SDA1YlU5VlhsUkhUYTgKWHRLWmJEdU01Y211QWlBb3FoY043NVZCQjNtclpJZlgyMVVrWHB3c2x6VjJ1ZkNQellWRVdIMFBCdz09Ci0tLS0tRU5EIENFUlRJRklDQVRFLS0tLS0K
    client-key-data: LS0tLS1CRUdJTiBFQyBQUklWQVRFIEtFWS0tLS0tCk1IY0NBUUVFSVA1dzV4U0MvdHNSTWpNTlN0TC9jMy9KZnFCdGRoeVZxNnh3R1VmY3l2S1VvQW9HQ0NxR1NNNDkKQXdFSG9VUURRZ0FFUFh6b1hCZyt2bEZlNVRTTzNYcTdJWm1HbUNnVXYxZkM1cFNuY1puTVN2UWVYdUZRaVplWgoxQnVKNUx4dm5FTy9QZVZ4enQ4NVNDN3hqY052V010clVBPT0KLS0tLS1FTkQgRUMgUFJJVkFURSBLRVktLS0tLQo=
- name: gke_godata-integration_asia-east1_g-godata-id-sourcing
  user:
    exec:
      apiVersion: client.authentication.k8s.io/v1beta1
      args: null
      command: gke-gcloud-auth-plugin
      env: null
      installHint: Install gke-gcloud-auth-plugin for use with kubectl by following
        https://cloud.google.com/blog/products/containers-kubernetes/kubectl-auth-changes-in-gke
      interactiveMode: IfAvailable
      provideClusterInfo: true
- name: gke_godata-platform_asia-southeast1_g-godata-systems-console
  user:
    exec:
      apiVersion: client.authentication.k8s.io/v1beta1
      args: null
      command: gke-gcloud-auth-plugin
      env: null
      installHint: Install gke-gcloud-auth-plugin for use with kubectl by following
        https://cloud.google.com/blog/products/containers-kubernetes/kubectl-auth-changes-in-gke
      interactiveMode: IfAvailable
      provideClusterInfo: true
- name: gke_godata-platform_asia-southeast1_p-godata-systems-console
  user:
    exec:
      apiVersion: client.authentication.k8s.io/v1beta1
      args: null
      command: gke-gcloud-auth-plugin
      env: null
      installHint: Install gke-gcloud-auth-plugin for use with kubectl by following
        https://cloud.google.com/blog/products/containers-kubernetes/kubectl-auth-changes-in-gke
      interactiveMode: IfAvailable
      provideClusterInfo: true
- name: gke_godata-production_asia-east1_p-godata-id-batching
  user:
    exec:
      apiVersion: client.authentication.k8s.io/v1beta1
      args: null
      command: gke-gcloud-auth-plugin
      env: null
      installHint: Install gke-gcloud-auth-plugin for use with kubectl by following
        https://cloud.google.com/blog/products/containers-kubernetes/kubectl-auth-changes-in-gke
      interactiveMode: IfAvailable
      provideClusterInfo: true
- name: gke_pilotdata-integration_asia-southeast1_g-pilotdata-gl-sourcing
  user:
    exec:
      apiVersion: client.authentication.k8s.io/v1beta1
      args: null
      command: gke-gcloud-auth-plugin
      env: null
      installHint: Install gke-gcloud-auth-plugin for use with kubectl by following
        https://cloud.google.com/blog/products/containers-kubernetes/kubectl-auth-changes-in-gke
      interactiveMode: IfAvailable
      provideClusterInfo: true`

	config, err := clientcmd.RESTConfigFromKubeConfig([]byte(kubeConfig))
	if err != nil {
		panic(err.Error())
	}

	// Create the clientSet
	clientSet, err := kubernetes.NewForConfig(config)
	if err != nil {
		panic(err.Error())
	}

	// Set TTL to 60 seconds after the job completes
	jobTTLAfterFinish := int32(60)

	// Define the Job spec
	executionJob := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name: "optimus-test-container-gcs-log-push",
		},
		Spec: batchv1.JobSpec{
			TTLSecondsAfterFinished: &jobTTLAfterFinish, // Auto-delete after completion
			Template: corev1.PodTemplateSpec{
				Spec: corev1.PodSpec{
					RestartPolicy: corev1.RestartPolicyNever,
					Containers: []corev1.Container{
						{
							Name:            "optimus-test-container",
							Image:           "optimus-dev",
							ImagePullPolicy: "Never",
							Lifecycle: &corev1.Lifecycle{
								PreStop: &corev1.Handler{
									Exec: &corev1.ExecAction{
										Command: []string{"/bin/sh", "/opt/exit.sh", "preStop", ">", "/etc/preStop.log"},
									},
									//HTTPGet: &corev1.HTTPGetAction{
									//	Path: "/fromHttpGet/preStop",
									//	Port: intstr.IntOrString{
									//		Type:   0,
									//		IntVal: 9000,
									//	},
									//	Host:   "192.168.0.128",
									//	Scheme: "http",
									//},
								},
								PostStart: &corev1.Handler{
									Exec: &corev1.ExecAction{
										Command: []string{"/bin/sh", "/opt/exit.sh", "postStart", ">", "/etc/postStart.log"},
									},
								},
							},
						},
					},
				},
			},
		},
	}

	// Create the Job
	jobClient := clientSet.BatchV1().Jobs("default") // Assuming the job is in the 'default' namespace
	jobClient.List()
	createdJob, err := jobClient.Create(context.TODO(), executionJob, metav1.CreateOptions{})
	if err != nil {
		panic(err.Error())
	}
	// store job run ide against generated name
	fmt.Printf("generated name : %s ,  object meta : %#v \n", createdJob.GetGenerateName(), createdJob.GetObjectMeta())

	fmt.Printf("Created job %q.\n", createdJob.GetObjectMeta().GetName())

	//select {}
}

func WatchPodEvents() {

	fmt.Println("started pod watcher")

	kubeConfig := `
apiVersion: v1
clusters:
- cluster:
    certificate-authority-data: LS0tLS1CRUdJTiBDRVJUSUZJQ0FURS0tLS0tCk1JSUJlRENDQVIyZ0F3SUJBZ0lCQURBS0JnZ3Foa2pPUFFRREFqQWpNU0V3SHdZRFZRUUREQmhyTTNNdGMyVnkKZG1WeUxXTmhRREUzTURnMk1ESXpNVFF3SGhjTk1qUXdNakl5TVRFME5URTBXaGNOTXpRd01qRTVNVEUwTlRFMApXakFqTVNFd0h3WURWUVFEREJock0zTXRjMlZ5ZG1WeUxXTmhRREUzTURnMk1ESXpNVFF3V1RBVEJnY3Foa2pPClBRSUJCZ2dxaGtqT1BRTUJCd05DQUFRL2JUdEtFdlErektWbE9qc3lSaGdITThmQitaL21JdWtOakE2TEU1ZS8KYWZZTEd2NThneGRGemkvZjRyM0RyV2lsS3pTckFlRjE0OW1NSU5wR1hJWUNvMEl3UURBT0JnTlZIUThCQWY4RQpCQU1DQXFRd0R3WURWUjBUQVFIL0JBVXdBd0VCL3pBZEJnTlZIUTRFRmdRVWYzc05qUDhGZCtVOXZsNWxoSHZMClVzMVhYaDR3Q2dZSUtvWkl6ajBFQXdJRFNRQXdSZ0loQU0xL2xKN1ZmekQzTFRYR1VoSy9GczgxSTdhQjJjMW0KOVEyTmxpK2V2Y0d0QWlFQW84NGY5b2x3OTdPYlNBTHM5cWw1dEVTZ0JwRVFwYmJQTXdkZGpXeW5GSjg9Ci0tLS0tRU5EIENFUlRJRklDQVRFLS0tLS0K
    server: https://127.0.0.1:6443
  name: colima
- cluster:
    certificate-authority-data: LS0tLS1CRUdJTiBDRVJUSUZJQ0FURS0tLS0tCk1JSUVMRENDQXBTZ0F3SUJBZ0lRU240QmRPQW9pR01JNHEwUVZYYzBuVEFOQmdrcWhraUc5dzBCQVFzRkFEQXYKTVMwd0t3WURWUVFERXlSbVlUYzRPVGd5WXkweFlqVmxMVFJqWldFdFlqSTNOeTFrTnpJMk56azJOelUxTlRVdwpJQmNOTWpNd05EQTJNRGt4TVRFeldoZ1BNakExTXpBek1qa3hNREV4TVROYU1DOHhMVEFyQmdOVkJBTVRKR1poCk56ZzVPREpqTFRGaU5XVXROR05sWVMxaU1qYzNMV1EzTWpZM09UWTNOVFUxTlRDQ0FhSXdEUVlKS29aSWh2Y04KQVFFQkJRQURnZ0dQQURDQ0FZb0NnZ0dCQU1qbE15ZTlMQkozRzdXdEhBenRBazl6MlBoZ1krendBRnNGSmYvTAphRkRqb3I3YUtlUVJudHJvd1gxZDhHdmo5emt1QjZnWFNGL25GOE5XS01BVXpTZnk1RXlTZ1g2UnNtU0NMa1ZkCkh1N0w0eENmNDJzc1VFL05oSjBtLzNEUTBHd3FLSW1WTTVvTGhZT3FsNEp2b29FaTNmbEx3R0M5RTBxaThrMWUKSEk0U1dtUFNOOFQ4bS9BcEsyZjZFakhXM2tBeldLVWUxNTdKV01CT1JxRU5MSFU3WW9DT1RDUEhub0ZzdFFJdApZaXpRWUZlUDZJSktodDZmNU9CaE1KVC80MXV4SGlsRHczcUFqSHh4Ukx1YXhkbGprQ1BXYWNMSnY1cStScDdzCnFqbzBuN2xpYWtyQzhlNkJNZndBV1RlaTV0V3VEWlRyMWtzT251YXNlaXJqUzRRZzZMNjB6UU4rbWxNVEJjUUQKSHU2VFkvT29RaFJmdVpTL09CT2V2cHJxWWVlSUx5USsyVGtOdGJLY2lGbmFINTZyUVgwclUvUS9HdXVEcjBZUQpZKyt0Z1RDY3hPVHlJdWpsU1lXUHh4REJ5N2UvazdDVGhPZGNKa05oa0ptRWFJZU1WSW5OUzJNc1J1amdWY2plCkVzOWcwK0tFVjhEc081TUlibFg4a3RrUEt3SURBUUFCbzBJd1FEQU9CZ05WSFE4QkFmOEVCQU1DQWdRd0R3WUQKVlIwVEFRSC9CQVV3QXdFQi96QWRCZ05WSFE0RUZnUVV4MmI5ZlNOVVVYbXRpcUVDMHNWMnFURDdJT293RFFZSgpLb1pJaHZjTkFRRUxCUUFEZ2dHQkFHZndETFg3Z25ZRk1UQnJMREtjNEZBaER1MTVnVTV4SnhTNk5WN05YdkM3Ck1KTTJrQU5sMStVMGJLL0VqcW1SWkxMeVY4UWl5UGE0aHpreWdSZjFNNFBBZ0dtWmMzc3REME9pZjdsbHF3cW8KZURCcEtlMGlkdlpiQnM1MUtQMVpJY0tmOGVLMjVNaENmd1BEaWpMUnkrMEtObk1Yc29SaVMvcksxK2w5ZUhpZwphSGxsZUI2L0gvQ0J2ZnQxODFlT0d0aTlrR2JYVVVQc0dXNUR1V2ZiQUlWREQyWDhuUU44WFIrenlXaFdzbW44CnV5SlQwSlJTQzI4TnFOOHREVTZwdjhGd0Z2NHFMb3pKRWt1R29ITGk4TTd6eVlQZDJleWdoQUZWNDYwV2ZubDEKWkdjTnpVOXJnU2VPTXF5aGlHWGI3ZGNWTWdMTXVneWhXVithem5xR1pudzRma2t6b1NvZDZXVVFkcmRuejR0SgpDMFc5aDZseFhZVnB4eWQyZ3JIWWZFRzZZQ2UwZnlHaXZFZ3grLzZhYWFLOXBMamY2ZU8zWWNzUFNta2tSVkJ6CkVLeDlkSUw2TjlHUUh0dFdJdVJFdVBzWWVlUlVqczhxcHE2a1FZZmh4QTdWVjNzU3lDMnhrU1dZazhiN3RkcTYKQ2k4RndJTy9qR2xkUWkzS1lQVWRzUT09Ci0tLS0tRU5EIENFUlRJRklDQVRFLS0tLS0K
    server: https://10.255.253.194
  name: gke_godata-integration_asia-east1_g-godata-id-sourcing
- cluster:
    certificate-authority-data: LS0tLS1CRUdJTiBDRVJUSUZJQ0FURS0tLS0tCk1JSURDekNDQWZPZ0F3SUJBZ0lRR0N5S2hNVk1UL1pyOFpuYldVZFBDakFOQmdrcWhraUc5dzBCQVFzRkFEQXYKTVMwd0t3WURWUVFERXlRM1pUTm1PVGMzWXkwMVpEUXlMVFF3Wm1NdFlqVmtPUzA0T1dObU1qSmxNMkV5TldVdwpIaGNOTVRreE1qQTBNRGt4TnpFeVdoY05NalF4TWpBeU1UQXhOekV5V2pBdk1TMHdLd1lEVlFRREV5UTNaVE5tCk9UYzNZeTAxWkRReUxUUXdabU10WWpWa09TMDRPV05tTWpKbE0yRXlOV1V3Z2dFaU1BMEdDU3FHU0liM0RRRUIKQVFVQUE0SUJEd0F3Z2dFS0FvSUJBUUMzR3c5akJlS0hzUFBwYzc3UkcwOEcxdkh0Y0t4M29ZcCtoNVA1MzFxQgplU093UDVORy8rY2dTaThQeXlTTG15Z2cwT2tmNXJxVzRCd2ZVMUJ0cU9YL1phMk03WUUxdnBlUWQvbnQ4UkN3ClZ3Z0tKT0RQNjUzQzhjMUxnN0s3SXFuVGl5eXRCcFhCckw2OWpvRTl2VHF2RXpIbHZrSklMOW5jazZvUlp1Q1IKZWtieks1dnM2VC8rSnROQ2lEUWZBcCtneFBlakZIYVRwNUVlNnZJTlEwTGRLRzB4RGZ2dmxXdDV1NXAxZnF6MApiTG1laDFuK0lSKytFdkhQOHJoaGU5UVlKcjZxbjJEVjlIS2hWb051cHN6eG5nTG5DS0tnN1NFU0w5ZUI3Und2CnZCNW9rQjRqTkk4b3ZHYXUwOFFYdDBaNWRXalRrTitGalN1cDVQZHNDM1YzQWdNQkFBR2pJekFoTUE0R0ExVWQKRHdFQi93UUVBd0lDQkRBUEJnTlZIUk1CQWY4RUJUQURBUUgvTUEwR0NTcUdTSWIzRFFFQkN3VUFBNElCQVFBSgorTXJOWEpiVEJsYVozV2RFRGNwQTVLQ3JKaFVJUjgvay9idCt1WmhXTU15ZVpoclBJN09GL3FJUnRGNnU2TXdvClRmaXZpTTlNck9EZHVwNGJCcUExR05KdWhjZklXcmp3cVcyam1KVE5iUjhIaWpwSWkzbXJCZkxSMWwvcUJFSDQKeHFXNnBiRWJYVEttQkx0L1ZYcU1IcGhydU52VXoybHpkbmYwOHM2ZTJRSTZ1U0Y1MSs2Zlg3Zi9KUlArODRVRApXQ2dmUWhNOUJPWDVlR1ZUL25FMVZvVnFqQmdMeEZVRmVqLzdHbmEzUTZVTVdDcnlQRW83S2lmblZ0YW1zU2lHClFLU0JXTXF4aFh5dTBtZ2RIa21DQldqRFBtWENwK1dIMGttczFmVzk2MWIwVEZLK0Y1Rm1jZW5HNWpiZ0NmeFgKZFZlZUhtVnNYakxqMnRyeEwvNjAKLS0tLS1FTkQgQ0VSVElGSUNBVEUtLS0tLQo=
    server: https://10.255.255.130
  name: gke_godata-platform_asia-southeast1_g-godata-systems-console
- cluster:
    certificate-authority-data: LS0tLS1CRUdJTiBDRVJUSUZJQ0FURS0tLS0tCk1JSURDekNDQWZPZ0F3SUJBZ0lRZVF0MDhCTUdYbHZqT2N4aGxEd2o4VEFOQmdrcWhraUc5dzBCQVFzRkFEQXYKTVMwd0t3WURWUVFERXlRM00yUXlNRFE0TnkwMFkyRXhMVFF4TXpVdE9XSXhPQzFqWWpGbE9EZGxZakZsWkRVdwpIaGNOTVRreE1qQTBNVEF5TWpRNFdoY05NalF4TWpBeU1URXlNalE0V2pBdk1TMHdLd1lEVlFRREV5UTNNMlF5Ck1EUTROeTAwWTJFeExUUXhNelV0T1dJeE9DMWpZakZsT0RkbFlqRmxaRFV3Z2dFaU1BMEdDU3FHU0liM0RRRUIKQVFVQUE0SUJEd0F3Z2dFS0FvSUJBUUM3YU93NjZJUkJOdWJ6OE1LWHRIdUJYU2tNdFlUUHlIZDgrMXZkRmlpbgplSEhtMnU0Q0RsNERpd0loM3ZkMVJJcnJvZVBKSDhnSkdQbC8xY0ZhQktBczBTQ0taZVdwT1FsdE1LN0U0NVplCjVDNDRpNUJtaGJQell1M2xaMDJXWEJXQ3Y2dTl1a2syd0cvenJTV3M0KzE4bk8zQ0ZvUHpJL2FaQzJPOTBpc1oKMGFzNGVuOVRTczV0d0RqV2lJbnRESWR5djNYZG5GUDJmY1M1cVR0MjlrbHRJOXFoZlRQd2dyU29yUFlUd250ZgpLOXdzbk12ZzRyYnpDbEVUeEh3Tm5ISTNrRjRNMWNqTnFySkRnMWd5MTFldmxRT0p0NUN2MHBxVW1pcC9SSURKCjFmQzNWcXQ3SXJXZkVaS3VCWDNOWVBUNlhqU1hsMGFoRDJscFRzMTgza2hQQWdNQkFBR2pJekFoTUE0R0ExVWQKRHdFQi93UUVBd0lDQkRBUEJnTlZIUk1CQWY4RUJUQURBUUgvTUEwR0NTcUdTSWIzRFFFQkN3VUFBNElCQVFBZQpPSWxFWTFuemt0Mm1pRnMrNDBLTDJkSXRSOVZHR05YWGNTVEs3Si9oZmJXNERtQ0JNU1BxOXFBMjJBN2R6WUVGCmlNcnc1UEdqSkcxcjM1cXNKbUJMVGdJRFFBNEdSbXAwVzZ0QnplMEtHVFNoYmZSV2tIeGRmbm9qcGxrWGZKcVAKTGlVQXYxK3V2ME9TeE4weFBob2FHUHBCOUF6bXRLR2tFWmd0R2YrTWdxZnp0Ky8yUDlJTHZCZ1ZFdFVnSTJxTApKY1JPMEZPdW80WmR4anBBc20rS21TdXFKbzNrcnFXdytFYzRIR3ovUnQwRUR3ekRabGZIaExreXk3RjNLS2t6CklXNE15RCthOUVCaVluUFlmelpCdkUrMDM4c3FmNjZjRVJtZHdFbnZJejNBVTU2SnVobzJRaE5VWjdtaVE5bU8KeVZTNW9sTCtUbjQ4eG14dnlBSk0KLS0tLS1FTkQgQ0VSVElGSUNBVEUtLS0tLQo=
    server: https://10.255.255.146
  name: gke_godata-platform_asia-southeast1_p-godata-systems-console
- cluster:
    certificate-authority-data: LS0tLS1CRUdJTiBDRVJUSUZJQ0FURS0tLS0tCk1JSURERENDQWZTZ0F3SUJBZ0lSQU1hc3RUSmJlU1c4VEVpOWJZd1VDT2N3RFFZSktvWklodmNOQVFFTEJRQXcKTHpFdE1Dc0dBMVVFQXhNa056aGpORFZrTXpjdE5tUmtPUzAwWXpGa0xUaGlaR1l0WmpFNU5tUTNaVFV6TlRobApNQjRYRFRFNU1UQXhOakEwTlRBMU5Wb1hEVEkwTVRBeE5EQTFOVEExTlZvd0x6RXRNQ3NHQTFVRUF4TWtOemhqCk5EVmtNemN0Tm1Sa09TMDBZekZrTFRoaVpHWXRaakU1Tm1RM1pUVXpOVGhsTUlJQklqQU5CZ2txaGtpRzl3MEIKQVFFRkFBT0NBUThBTUlJQkNnS0NBUUVBNWZoeHl5TS83UUxDa1VacVdaWXQ1N0pkYzdYRDBET1pSdThkUDVvWQpzTGpVTDBlbTBWTFZsR3Q1aVAyTCswSmVkQ095ZktESmY2d3UzLzNEVHhaanN1UVE2M00rTjEwdW1lZ2d3anN3Ck5DM3l1N01XbllpUkFCNjA2THkwOHpkZnNCdmo3NVUrbWN4bmVFWFdsT2ZHK1Z0VXZBaDNJWGVDc3pjL0RVRDYKN1gxdHdsaStuaUpxaUFUS2h3b1VEdXhBb2R2LzFSYVlWN29uTlVwQkhaT2U1UkIveHh1MWh1RlJLbCtISkJkNwpQb0EwbWdzZmhNeWhGKzVVNG5OVTloS2xDM2FCWkRySCtQQjh3TWNFT3dwVEt5Zzh6WkZkNDU2S0NYRXhYZ2dwCmNWTXpyNE5RcnR1WFhQcFdzOVJkbGN4enFvWFdaejBaN2JlRjlzajFxa3Bhc1FJREFRQUJveU13SVRBT0JnTlYKSFE4QkFmOEVCQU1DQWdRd0R3WURWUjBUQVFIL0JBVXdBd0VCL3pBTkJna3Foa2lHOXcwQkFRc0ZBQU9DQVFFQQppeDFUVUpIR1FUeTQrdHpodnBmWmxXTVBGS05XV0F5T3NrekRYcDhaWDlPTng3SHpoVzJjTTVRdG0rZ1huWGorCnp3M1cwdlFzblpoQzYwakZ3Z1hnNEtPc3hVanhicUVRZm10dTNEMWJlcUVRaGZkaVpJT3liNWNuVjJvQkVkRnUKY2c5L1B5MEZCUzEzZlVsRWlXWWZOaHpwdDQ0RldERjNxZGgzbXlvNjErTmRNZU02Ryt6Z245REUyWCt3c2JnRQpTUnZjNmlTNUwyOU5vN1lVVUlRWkN3c2Vua3ZvZHZrNE9mM0tXM0RyTHhjRUM2Qk1WWlFxMW15RFlEVnNvYlNDCkJYRFM1WlhRZ2NtelVGUmlUNEt6cXFJbzZpUng1c2dFWVpFQ20weEFvbmJmRU03RVg2Nk9LTmQvVkNBNVlEM08KcTFjZHZiUERTeW1FUmM5aEdNTDJqUT09Ci0tLS0tRU5EIENFUlRJRklDQVRFLS0tLS0K
    server: https://10.255.253.114
  name: gke_godata-production_asia-east1_p-godata-id-batching
- cluster:
    certificate-authority-data: LS0tLS1CRUdJTiBDRVJUSUZJQ0FURS0tLS0tCk1JSURLekNDQWhPZ0F3SUJBZ0lSQUtjZFJ0TEJuZXBBOWNBc2g2ZkdjRUV3RFFZSktvWklodmNOQVFFTEJRQXcKTHpFdE1Dc0dBMVVFQXhNa1pUQmxOelpoTjJJdE1HWXhaQzAwTVRjNExUazJOMk10WlRoa05HSXpaakkwTldVNApNQjRYRFRJd01UQXlOakF6TlRneU1Gb1hEVEkxTVRBeU5UQTBOVGd5TUZvd0x6RXRNQ3NHQTFVRUF4TWtaVEJsCk56WmhOMkl0TUdZeFpDMDBNVGM0TFRrMk4yTXRaVGhrTkdJelpqSTBOV1U0TUlJQklqQU5CZ2txaGtpRzl3MEIKQVFFRkFBT0NBUThBTUlJQkNnS0NBUUVBbFRrRzcvNE9PNEo4OEtXbXFwbnZIRHc3UkFMUlcxd1NYeVFER2hZawpWZWRSbGZzQS9xZnJtL2VVbzFDVFAxT0FDTituTFVwU0V4UEJpU3BYV1BRYjd3dGt2ekcxL0RQKytjQStMM1Q5CjZmckJtTEtSSDZNTDVHdHVWL1BTNzhmKzBaQkR0SmNDdWFzekl4Q1JnbEFwMEJZRlg4Ymd3R0QxTmpLVUg2bkMKYnBLWXBUK2ZGQzNUTlR3MkxIeCtSZytIQzdQdTcvRm00RnFRS0xnRUNYVm5Tcks3K0JDajRlWnVncko4TU0wagpVYTYydkxoa2ZqSDlRL3l4UHVTMVdkMVFwMFlyL1ZzWmg5MlJrN2VtY1E2eXZhTXB0K3BYYzNkMVhFekphRFAzCnVkdysyRkFaZVcxRGF0WjhRVVZ5c3BPWUJPSnNSNmtPd2lRVFVTM25iRHpwd1FJREFRQUJvMEl3UURBT0JnTlYKSFE4QkFmOEVCQU1DQWdRd0R3WURWUjBUQVFIL0JBVXdBd0VCL3pBZEJnTlZIUTRFRmdRVWFGV0s5REIvNDR2UApZZFF3SWFHL3o1YUZZZzR3RFFZSktvWklodmNOQVFFTEJRQURnZ0VCQUJSNHhYL0FhRHhMZ3hydDVSZDZabjBrCmVLemYzcFVpcEZPYzFMUDZ5OEZmR1JscVJ0UWJlN3NSd1c1M09rMHI0dk1GSDNNY3J2KzBsM2dvdWlrd0JxR1IKNEk3a25UNGlvWkg0QVY3bFVhVVVNOHpCTFh1U0VTalBlZVo2YXFoY0ZueWNXL0lYdWx6cGFjbXhITm5LSXN3aQp2bC9wL0tkM1Zmakh5TGlUVUk5Ylh3TzNsRE9SbFlPTEg2TDd5ZGw1ZHRtUmM3cVBIbkdXMm81LzNJVTVINkJyCkNuWHQ2YlI4YzlOT0N6TGtmYVUvVWRKSjRpeXpKZ1Bvb09yaUUwOHlmbG5yaVRwZWpXZmdGTEhxSkpCaUJReTIKYy85MHVyOGc1OFZzS0hqN20rbTRGcC9GY3VWOHl6NVFVL0V2Q21McXZXTCtOMXRocU04bTg3cjNGVmxBc1hVPQotLS0tLUVORCBDRVJUSUZJQ0FURS0tLS0tCg==
    server: https://10.109.129.130
  name: gke_pilotdata-integration_asia-southeast1_g-pilotdata-gl-sourcing
contexts:
- context:
    cluster: colima
    user: colima
  name: colima
- context:
    cluster: gke_godata-integration_asia-east1_g-godata-id-sourcing
    user: gke_godata-integration_asia-east1_g-godata-id-sourcing
  name: gke_godata-integration_asia-east1_g-godata-id-sourcing
- context:
    cluster: gke_godata-platform_asia-southeast1_g-godata-systems-console
    user: gke_godata-platform_asia-southeast1_g-godata-systems-console
  name: gke_godata-platform_asia-southeast1_g-godata-systems-console
- context:
    cluster: gke_godata-platform_asia-southeast1_p-godata-systems-console
    namespace: optimus
    user: gke_godata-platform_asia-southeast1_p-godata-systems-console
  name: gke_godata-platform_asia-southeast1_p-godata-systems-console
- context:
    cluster: gke_godata-production_asia-east1_p-godata-id-batching
    user: gke_godata-production_asia-east1_p-godata-id-batching
  name: gke_godata-production_asia-east1_p-godata-id-batching
- context:
    cluster: gke_pilotdata-integration_asia-southeast1_g-pilotdata-gl-sourcing
    user: gke_pilotdata-integration_asia-southeast1_g-pilotdata-gl-sourcing
  name: gke_pilotdata-integration_asia-southeast1_g-pilotdata-gl-sourcing
current-context: colima
kind: Config
preferences: {}
users:
- name: colima
  user:
    client-certificate-data: LS0tLS1CRUdJTiBDRVJUSUZJQ0FURS0tLS0tCk1JSUJrRENDQVRlZ0F3SUJBZ0lJV09BbGJvM0g4Q2d3Q2dZSUtvWkl6ajBFQXdJd0l6RWhNQjhHQTFVRUF3d1kKYXpOekxXTnNhV1Z1ZEMxallVQXhOekE0TmpBeU16RTBNQjRYRFRJME1ESXlNakV4TkRVeE5Gb1hEVEkxTURJeQpNVEV4TkRVeE5Gb3dNREVYTUJVR0ExVUVDaE1PYzNsemRHVnRPbTFoYzNSbGNuTXhGVEFUQmdOVkJBTVRESE41CmMzUmxiVHBoWkcxcGJqQlpNQk1HQnlxR1NNNDlBZ0VHQ0NxR1NNNDlBd0VIQTBJQUJEMTg2RndZUHI1Ulh1VTAKanQxNnV5R1pocGdvRkw5WHd1YVVwM0daekVyMEhsN2hVSW1YbWRRYmllUzhiNXhEdnozbGNjN2ZPVWd1OFkzRApiMWpMYTFDalNEQkdNQTRHQTFVZER3RUIvd1FFQXdJRm9EQVRCZ05WSFNVRUREQUtCZ2dyQmdFRkJRY0RBakFmCkJnTlZIU01FR0RBV2dCUkE4emtQaXlUTmlwRmZGNTcvNlJoT09DQkpNVEFLQmdncWhrak9QUVFEQWdOSEFEQkUKQWlBeU5pRWdvaDcwcnd4WDUwTWZrdmV4ZmduRlRHc1RBSmRkcG1yL3RGRGFsUUlnY3BPNDl5bVBFam5zakw0SQpycnhlQzRxUVEyeUdTL1JSWGVRUkxaOUtCMWc9Ci0tLS0tRU5EIENFUlRJRklDQVRFLS0tLS0KLS0tLS1CRUdJTiBDRVJUSUZJQ0FURS0tLS0tCk1JSUJkekNDQVIyZ0F3SUJBZ0lCQURBS0JnZ3Foa2pPUFFRREFqQWpNU0V3SHdZRFZRUUREQmhyTTNNdFkyeHAKWlc1MExXTmhRREUzTURnMk1ESXpNVFF3SGhjTk1qUXdNakl5TVRFME5URTBXaGNOTXpRd01qRTVNVEUwTlRFMApXakFqTVNFd0h3WURWUVFEREJock0zTXRZMnhwWlc1MExXTmhRREUzTURnMk1ESXpNVFF3V1RBVEJnY3Foa2pPClBRSUJCZ2dxaGtqT1BRTUJCd05DQUFRcXJNRGNqeTB0eTJCMmxrcG5Vdmx6U1VHQTlVWU81YW1NMEp2WC9LMEwKdlNISzZaUnQzYS9MRGdPbUl4ZmYwaGJaL0JaN0hsNnZmdXJFR0MwTXVoM2NvMEl3UURBT0JnTlZIUThCQWY4RQpCQU1DQXFRd0R3WURWUjBUQVFIL0JBVXdBd0VCL3pBZEJnTlZIUTRFRmdRVVFQTTVENHNrellxUlh4ZWUvK2tZClRqZ2dTVEV3Q2dZSUtvWkl6ajBFQXdJRFNBQXdSUUloQVBnOHJ4elBYbU9uU0N6TlN5SDA1YlU5VlhsUkhUYTgKWHRLWmJEdU01Y211QWlBb3FoY043NVZCQjNtclpJZlgyMVVrWHB3c2x6VjJ1ZkNQellWRVdIMFBCdz09Ci0tLS0tRU5EIENFUlRJRklDQVRFLS0tLS0K
    client-key-data: LS0tLS1CRUdJTiBFQyBQUklWQVRFIEtFWS0tLS0tCk1IY0NBUUVFSVA1dzV4U0MvdHNSTWpNTlN0TC9jMy9KZnFCdGRoeVZxNnh3R1VmY3l2S1VvQW9HQ0NxR1NNNDkKQXdFSG9VUURRZ0FFUFh6b1hCZyt2bEZlNVRTTzNYcTdJWm1HbUNnVXYxZkM1cFNuY1puTVN2UWVYdUZRaVplWgoxQnVKNUx4dm5FTy9QZVZ4enQ4NVNDN3hqY052V010clVBPT0KLS0tLS1FTkQgRUMgUFJJVkFURSBLRVktLS0tLQo=
- name: gke_godata-integration_asia-east1_g-godata-id-sourcing
  user:
    exec:
      apiVersion: client.authentication.k8s.io/v1beta1
      args: null
      command: gke-gcloud-auth-plugin
      env: null
      installHint: Install gke-gcloud-auth-plugin for use with kubectl by following
        https://cloud.google.com/blog/products/containers-kubernetes/kubectl-auth-changes-in-gke
      interactiveMode: IfAvailable
      provideClusterInfo: true
- name: gke_godata-platform_asia-southeast1_g-godata-systems-console
  user:
    exec:
      apiVersion: client.authentication.k8s.io/v1beta1
      args: null
      command: gke-gcloud-auth-plugin
      env: null
      installHint: Install gke-gcloud-auth-plugin for use with kubectl by following
        https://cloud.google.com/blog/products/containers-kubernetes/kubectl-auth-changes-in-gke
      interactiveMode: IfAvailable
      provideClusterInfo: true
- name: gke_godata-platform_asia-southeast1_p-godata-systems-console
  user:
    exec:
      apiVersion: client.authentication.k8s.io/v1beta1
      args: null
      command: gke-gcloud-auth-plugin
      env: null
      installHint: Install gke-gcloud-auth-plugin for use with kubectl by following
        https://cloud.google.com/blog/products/containers-kubernetes/kubectl-auth-changes-in-gke
      interactiveMode: IfAvailable
      provideClusterInfo: true
- name: gke_godata-production_asia-east1_p-godata-id-batching
  user:
    exec:
      apiVersion: client.authentication.k8s.io/v1beta1
      args: null
      command: gke-gcloud-auth-plugin
      env: null
      installHint: Install gke-gcloud-auth-plugin for use with kubectl by following
        https://cloud.google.com/blog/products/containers-kubernetes/kubectl-auth-changes-in-gke
      interactiveMode: IfAvailable
      provideClusterInfo: true
- name: gke_pilotdata-integration_asia-southeast1_g-pilotdata-gl-sourcing
  user:
    exec:
      apiVersion: client.authentication.k8s.io/v1beta1
      args: null
      command: gke-gcloud-auth-plugin
      env: null
      installHint: Install gke-gcloud-auth-plugin for use with kubectl by following
        https://cloud.google.com/blog/products/containers-kubernetes/kubectl-auth-changes-in-gke
      interactiveMode: IfAvailable
      provideClusterInfo: true`

	config, err := clientcmd.RESTConfigFromKubeConfig([]byte(kubeConfig))
	if err != nil {
		panic(err.Error())
	}

	// Create the clientSet
	clientSet, err := kubernetes.NewForConfig(config)
	if err != nil {
		panic(err.Error())
	}

	// Create a shared informer factory
	factory := informers.NewSharedInformerFactory(clientSet, time.Minute)

	// Create a pod informer
	podInformer := factory.Core().V1().Events().Informer()

	// Handle Pod events
	podInformer.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj interface{}) {
			event := obj.(*corev1.Event)
			fmt.Println("Pod added:", event.Name)
		},
		UpdateFunc: func(oldObj, newObj interface{}) {
			oldPod := oldObj.(*corev1.Event)
			newPod := newObj.(*corev1.Event)

			diff, err := utils.GetDiffs(oldPod, newPod, nil)
			if err != nil {
				fmt.Println(err.Error())
			}
			for _, d := range diff {
				fmt.Printf("DIFF: property: %s \n", d.Field)
				parts := strings.Split(d.Diff, "+")
				fmt.Printf("DIFF:      old: %s \n", parts[0])
				fmt.Printf("DIFF:      new: %s \n", parts[1])
			}
			fmt.Println("Pod updated:", oldPod.Name, "->", newPod.Name)
		},
		DeleteFunc: func(obj interface{}) {
			pod := obj.(*corev1.Event)
			fmt.Println("Pod deleted:", pod.Name)
		},
	})

	// Error handling and stopping
	stopCh := make(chan struct{})
	defer close(stopCh)

	defer runtime.HandleCrash()

	// Start the informer
	go podInformer.Run(stopCh)

	// Wait for the cache to sync
	if !cache.WaitForCacheSync(stopCh, podInformer.HasSynced) {
		fmt.Println("Error waiting for cache to sync")
		return
	}

	// Continue running until stopped
	<-stopCh

	fmt.Println("finished pod watcher")
}

func (s *JobRunService) UploadToScheduler(ctx context.Context, projectName tenant.ProjectName) error {
	spanCtx, span := otel.Tracer("optimus").Start(ctx, "UploadToScheduler")
	defer span.End()

	me := errors.NewMultiError("errorInUploadToScheduler")
	allJobsWithDetails, err := s.jobRepo.GetAll(spanCtx, projectName)
	me.Append(err)
	if allJobsWithDetails == nil {
		return me.ToErr()
	}
	span.AddEvent("got all the jobs to upload")

	err = s.priorityResolver.Resolve(spanCtx, allJobsWithDetails)
	if err != nil {
		s.l.Error("error resolving priority: %s", err)
		me.Append(err)
		return me.ToErr()
	}
	span.AddEvent("done with priority resolution")

	jobGroupByTenant := scheduler.GroupJobsByTenant(allJobsWithDetails)
	for t, jobs := range jobGroupByTenant {
		span.AddEvent("uploading job specs")
		if err = s.deployJobsPerNamespace(spanCtx, t, jobs); err == nil {
			s.l.Info("[success] namespace: %s, project: %s, deployed", t.NamespaceName().String(), t.ProjectName().String())
		}
		me.Append(err)

		span.AddEvent("uploading job metrics")
	}
	return me.ToErr()
}

func (s *JobRunService) deployJobsPerNamespace(ctx context.Context, t tenant.Tenant, jobs []*scheduler.JobWithDetails) error {
	err := s.scheduler.DeployJobs(ctx, t, jobs)
	if err != nil {
		s.l.Error("error deploying jobs under project [%s] namespace [%s]: %s", t.ProjectName().String(), t.NamespaceName().String(), err)
		return err
	}
	return s.cleanPerNamespace(ctx, t, jobs)
}

func (s *JobRunService) cleanPerNamespace(ctx context.Context, t tenant.Tenant, jobs []*scheduler.JobWithDetails) error {
	// get all stored job names
	schedulerJobNames, err := s.scheduler.ListJobs(ctx, t)
	if err != nil {
		s.l.Error("error listing jobs under project [%s] namespace [%s]: %s", t.ProjectName().String(), t.NamespaceName().String(), err)
		return err
	}
	jobNamesMap := make(map[string]struct{})
	for _, job := range jobs {
		jobNamesMap[job.Name.String()] = struct{}{}
	}
	var jobsToDelete []string

	for _, schedulerJobName := range schedulerJobNames {
		if _, ok := jobNamesMap[schedulerJobName]; !ok {
			jobsToDelete = append(jobsToDelete, schedulerJobName)
		}
	}
	return s.scheduler.DeleteJobs(ctx, t, jobsToDelete)
}

func (s *JobRunService) UpdateJobScheduleState(ctx context.Context, tnnt tenant.Tenant, jobName []job.Name, state string) error {
	return s.scheduler.UpdateJobState(ctx, tnnt, jobName, state)
}

func (s *JobRunService) UploadJobs(ctx context.Context, tnnt tenant.Tenant, toUpdate, toDelete []string) (err error) {
	me := errors.NewMultiError("errorInUploadJobs")

	if len(toUpdate) > 0 {
		if err = s.resolveAndDeployJobs(ctx, tnnt, toUpdate); err == nil {
			s.l.Info("[success] namespace: %s, project: %s, deployed %d jobs", tnnt.NamespaceName().String(),
				tnnt.ProjectName().String(), len(toUpdate))
		}
		me.Append(err)
	}

	if len(toDelete) > 0 {
		if err = s.scheduler.DeleteJobs(ctx, tnnt, toDelete); err == nil {
			s.l.Info("deleted %s jobs on project: %s", len(toDelete), tnnt.ProjectName())
		}
		me.Append(err)
	}

	return me.ToErr()
}

func (s *JobRunService) resolveAndDeployJobs(ctx context.Context, tnnt tenant.Tenant, toUpdate []string) error {
	allJobsWithDetails, err := s.jobRepo.GetJobs(ctx, tnnt.ProjectName(), toUpdate)
	if err != nil || allJobsWithDetails == nil {
		return err
	}

	if err := s.priorityResolver.Resolve(ctx, allJobsWithDetails); err != nil {
		s.l.Error("error priority resolving jobs: %s", err)
		return err
	}

	return s.scheduler.DeployJobs(ctx, tnnt, allJobsWithDetails)
}
