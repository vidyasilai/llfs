#pragma once
#ifndef LLFS_OPAQUE_PAGE_VIEW_HPP
#define LLFS_OPAQUE_PAGE_VIEW_HPP

#include <llfs/page_view.hpp>

namespace llfs {

class OpaquePageView : public PageView
{
 public:
  using PageView::PageView;

  // Get the tag for this page view.
  //
  PageLayoutId get_page_layout_id() const override;

  // Returns a sequence of the ids of all pages directly referenced by this one.
  //
  BoxedSeq<PageId> trace_refs() const override;

  // Returns the minimum key value contained within this page.
  //
  Optional<KeyView> min_key() const override;

  // Returns the maximum key value contained within this page.
  //
  Optional<KeyView> max_key() const override;

  // Builds a key-based approximate member query (AMQ) filter for the page, to answer the question
  // whether a given key *might* be contained by the page.
  //
  std::shared_ptr<PageFilter> build_filter() const override;

  // Dump a human-readable representation or summary of the page to the passed stream.
  //
  void dump_to_ostream(std::ostream& out) const override;
};

}  // namespace llfs

#endif  // LLFS_OPAQUE_PAGE_VIEW_HPP
