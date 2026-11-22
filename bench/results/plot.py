"""Draws docs/img/e3-latency-{tmpfs,disk}.svg from the rig outputs of a sweep (sweep19.sh: e3-rust-rig)
(run from the repository root). Percentiles on a "nines" axis, latency log."""
import re, math, sys
import matplotlib
matplotlib.use('svg')
import matplotlib.pyplot as plt
# Usage: python3 plot.py <results dir of sweep16.sh> (default: the one on nuc2)
E=sys.argv[1] if len(sys.argv) > 1 else '/var/tmp/krusty-aeron-bench/results-e3/rust-rig'
def load(p):
    t=open(p).read()
    if 'managed to send only' in t: return None
    rows=[]
    for line in t.splitlines():
        m=re.match(r'\s+([\d.]+)\s+([\d.]+)\s+(\d+)\s+([\d.]+|inf)',line)
        if m: rows.append((float(m.group(2)),float(m.group(1))))
    return rows
SURF='#fcfcfb'; INK='#1f1f1e'; MUTED='#6b6a64'; GRID='#e6e5e0'
COL={'Aeron':'#eb6834','KrustyCast':'#2a78d6'}
plt.rcParams.update({'font.size':9,'font.family':'DejaVu Sans','text.color':INK,'axes.labelcolor':MUTED,
    'xtick.color':MUTED,'ytick.color':MUTED,'axes.edgecolor':GRID,'svg.fonttype':'none'})
def nines(p): return -math.log10(max(1-p,1e-7))
ticks=[0,.9,.99,.999,.9999,.99999]; labels=['0%','90%','99%','99.9%','99.99%','99.999%']
for kind,title,fn in (('none','Logs on tmpfs (Aeron on tmpfs)','tmpfs'),('async','On disk (Aeron archive sync level 0; KrustyCast mode (a))','disk')):
    fig,axes=plt.subplots(1,4,figsize=(12,3.7),sharey=True,facecolor=SURF)
    for ax,r in zip(axes,(100000,300000,500000,750000)):
        ax.set_facecolor(SURF)
        ends={}
        for s,name in (('a','Aeron'),('k','KrustyCast')):
            rows=load(f'{E}/{s}-{kind}-{r}.txt')
            if rows is None:
                ax.text(0.04,0.93,'Aeron fell behind',transform=ax.transAxes,color=MUTED,fontsize=8.5); continue
            xs=[nines(p) for p,v in rows if p<1]; ys=[v for p,v in rows if p<1]
            ax.plot(xs,ys,color=COL[name],lw=2,solid_capstyle='round')
            ends[name]=(min(xs[-1],6.4),ys[-1])
        if len(ends)==2:
            (xa,ya),(xk,yk)=ends['Aeron'],ends['KrustyCast']
            if max(ya,yk)/min(ya,yk)<1.5:
                lo,hi=('Aeron','KrustyCast') if ya<=yk else ('KrustyCast','Aeron')
                m=math.sqrt(ya*yk); ends[lo]=(ends[lo][0],m/1.35); ends[hi]=(ends[hi][0],m*1.35)
        for name,(x,y) in ends.items():
            ax.text(x+0.1,y,name,color=INK,fontsize=8,va='center')
        ax.set_yscale('log'); ax.set_xticks([nines(p) for p in ticks]); ax.set_xticklabels(labels,rotation=45,ha='right')
        ax.set_xlim(0,6.6); ax.grid(True,color=GRID,lw=0.6); ax.set_axisbelow(True)
        for sp in ('top','right'): ax.spines[sp].set_visible(False)
        ax.set_title(f'{r//1000}k msg/s',color=INK,fontsize=10)
        ax.set_xlabel('percentile')
    axes[0].set_ylabel('latency, µs\n(scheduled send → answer)')
    axes[0].set_ylim(15,20000)
    axes[0].set_yticks([20,50,100,200,500,1000,2000,5000,10000]); axes[0].set_yticklabels(['20','50','100','200','500','1 ms','2 ms','5 ms','10 ms'])
    axes[0].yaxis.set_minor_formatter(matplotlib.ticker.NullFormatter())
    h=[plt.Line2D([],[],color=COL[n],lw=2) for n in COL]
    fig.legend(h,list(COL),loc='upper right',frameon=False,ncol=2)
    fig.suptitle(title,x=0.01,ha='left',fontsize=11,color=INK)
    fig.tight_layout(rect=(0,0,0.985,0.93))
    fig.savefig(f"docs/img/e3-latency-{fn}.svg",facecolor=SURF)
    
